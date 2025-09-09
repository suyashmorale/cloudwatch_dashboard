import os, time, json, re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import boto3
import streamlit as st
import pandas as pd

# --------- Config (env or defaults) ----------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
LOG_GROUPS = [g.strip() for g in os.getenv("LOG_GROUPS", "/ecs/stage-agent-backend-task").split(",") if g.strip()]
DEFAULT_MINUTES = int(os.getenv("DEFAULT_MINUTES", "60"))  # last 60 mins by default
LAMBDA_GROUP = os.getenv("LAMBDA_GROUP")
if LAMBDA_GROUP:
    LOG_GROUPS.append(LAMBDA_GROUP)

boto_session = boto3.Session(region_name=AWS_REGION)
logs = boto_session.client("logs")

# --------- Helpers ----------
def run_insights(query: str, start: datetime, end: datetime, log_groups: List[str], limit: int = 10000) -> List[Dict[str, str]]:
    qid = logs.start_query(
        logGroupNames=log_groups,
        startTime=int(start.timestamp()),
        endTime=int(end.timestamp()),
        queryString=query,
        limit=limit,
    )["queryId"]
    while True:
        out = logs.get_query_results(queryId=qid)
        status = out.get("status")
        if status in ("Complete", "Failed", "Cancelled"):
            break
        time.sleep(1)
    if status != "Complete":
        st.warning(f"Query did not complete: {status}")
        return []
    rows = []
    for r in out.get("results", []):
        row = {}
        for c in r:
            row[c.get("field")] = c.get("value")
        rows.append(row)
    return rows

def summary_rows(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """group by correlationId, pull latest sender/to/logStream/message."""
    query = r"""
fields @timestamp, @logStream, @message
| parse @message /["']correlationId["']\s*:\s*["'](?<cid_json>[^"']+)["']/
| parse @message /correlationId\s*[:=]\s*(?<cid_inline>[^,"'\s}}]+)/
| parse @message /["']senderEmail["']\s*:\s*["'](?<se_json>[^"']+)["']/
| parse @message /senderEmail\s*[:=]\s*(?<se_inline>[^,"'\s}}]+)/
| parse @message /["']toEmail["']\s*:\s*["'](?<te_json>[^"']+)["']/
| parse @message /toEmail\s*[:=]\s*(?<te_inline>[^,"'\s}}]+)/
| fields coalesce(cid_json, cid_inline) as correlationId,
         coalesce(se_json, se_inline) as senderEmail_v,
         coalesce(te_json, te_inline) as toEmail_v,
         @timestamp, @logStream, @message
| filter ispresent(correlationId)
| stats min(@timestamp) as firstTs,
        latest(senderEmail_v) as senderEmail,
        latest(toEmail_v) as toEmail,
        latest(@logStream) as logStream,
        latest(@message) as message
  by correlationId
| sort correlationId desc
| limit 500
""".strip()
    return run_insights(query, start, end, LOG_GROUPS)

def events_for_id(correlation_id: str, start: datetime, end: datetime) -> List[Dict[str, str]]:
    """All raw events for the chosen correlationId (across groups)."""
    query = r"""
fields @timestamp, @logGroup, @logStream, @message
| parse @message /["']correlationId["']\s*:\s*["'](?<cid_json>[^"']+)["']/
| parse @message /correlationId\s*[:=]\s*(?<cid_inline>[^,"'\s}}]+)/
| fields coalesce(cid_json, cid_inline) as correlationId, @timestamp, @logGroup, @logStream, @message
| filter correlationId = "{}"
| sort @timestamp asc
| limit 5000
""".strip().format(correlation_id)
    return run_insights(query, start, end, LOG_GROUPS)

# Extract only the "msg" value from a log line
def parse_msg_only(message: str) -> str:
    if not message:
        return ""
    s = message.strip()
    # If the line has a prefix like "INFO:root:{"..."}", drop everything before the first '{'
    brace = s.find("{")
    if brace != -1:
        json_part = s[brace:]
        try:
            obj = json.loads(json_part)
            if isinstance(obj, dict) and "msg" in obj:
                return str(obj.get("msg", ""))
        except Exception:
            pass  # fall through to patterns
    # Try to find msg in a JSON-ish substring: "msg":"...".
    m = re.search(r'"msg"\s*:\s*"([^"]+)"', s)
    if m:
        return m.group(1)
    # Try inline style: msg: something, (until comma or EOL)
    m2 = re.search(r'\bmsg\s*:\s*([^,}]+)', s)
    if m2:
        return m2.group(1).strip()
    # Fallback: return the whole message
    return s

def cw_stream_link(region: str, group: str, stream: str, ts_iso: str) -> str:
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
    except Exception:
        return ""
    start = int((ts - timedelta(minutes=5)).timestamp() * 1000)
    end   = int((ts + timedelta(minutes=5)).timestamp() * 1000)
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups/log-group/{group.replace('/','$252F')}"
        f"/log-events/{stream.replace('/','$252F')}?start={start}&end={end}"
    )

# --------- UI ----------
st.set_page_config(page_title="8vdx – Flow Logs", layout="wide")
st.title("8vdx · Flow Logs (CloudWatch)")

# Controls
c1, c2, c3, c4 = st.columns([1,1,1,1])
with c1:
    time_unit = st.selectbox("Time unit", ["Minutes", "Hours", "Days"], index=0)
with c2:
    if time_unit == "Minutes":
        time_value = st.slider("Time window", min_value=5, max_value=1440, value=DEFAULT_MINUTES, step=5)
        minutes = time_value
    elif time_unit == "Hours":
        time_value = st.slider("Time window", min_value=1, max_value=168, value=DEFAULT_MINUTES//60, step=1)
        minutes = time_value * 60
    else:  # Days
        time_value = st.slider("Time window", min_value=1, max_value=30, value=DEFAULT_MINUTES//1440, step=1)
        minutes = time_value * 24 * 60
with c3:
    auto = st.checkbox("Auto-refresh every 30s", value=False)
with c4:
    region = st.text_input("AWS Region", AWS_REGION)# Display time range info
time_display = f"{time_value} {time_unit.lower()}"
if time_unit == "Minutes" and time_value >= 60:
    hours = time_value // 60
    remaining_minutes = time_value % 60
    if remaining_minutes > 0:
        time_display += f" ({hours}h {remaining_minutes}m)"
    else:
        time_display += f" ({hours}h)"
elif time_unit == "Hours" and time_value >= 24:
    days = time_value // 24
    remaining_hours = time_value % 24
    if remaining_hours > 0:
        time_display += f" ({days}d {remaining_hours}h)"
    else:
        time_display += f" ({days}d)"

st.caption(f"Reading from log groups: {', '.join(LOG_GROUPS)} | Time window: {time_display}")
end = datetime.now(timezone.utc)
start = end - timedelta(minutes=minutes)

# ---------- BOX 1: Summary (like TXT1) ----------
st.subheader("Flows (grouped by correlationId)")
with st.spinner("Loading flows..."):
    flows = summary_rows(start, end)

if not flows:
    st.info("No flows found in the selected time window.")
    st.stop()

# Build summary DataFrame
summary_df = pd.DataFrame(flows)

# Define the columns we want to display, checking if they exist
display_columns = ["correlationId"]
optional_columns = ["senderEmail", "toEmail", "logStream", "message", "firstTs"]

# Add optional columns if they exist in the data
for col in optional_columns:
    if col in summary_df.columns:
        display_columns.append(col)

# Select only the columns that exist
summary_df = summary_df[display_columns]

# Rename firstTs if it exists
if "firstTs" in summary_df.columns:
    summary_df = summary_df.rename(columns={"firstTs": "startTime(UTC)"})

# Show summary table with clickable correlation IDs
st.markdown("**Click on any correlation ID in the table below to view its logs:**")
selected_rows = st.dataframe(
    summary_df, 
    use_container_width=True, 
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row"
)

# Get selected correlation ID
if selected_rows.selection.rows:
    selected_row_index = selected_rows.selection.rows[0]
    selected_id = summary_df.iloc[selected_row_index]["correlationId"]
    st.success(f"Selected correlation ID: **{selected_id}**")
else:
    # Default to first row if nothing is selected
    selected_id = summary_df.iloc[0]["correlationId"]
    st.info(f"Showing logs for: **{selected_id}** (click on a row to select a different correlation ID)")

# ---------- BOX 2: Details (msg only) ----------
st.subheader(f"Logs (msg only) for correlationId: {selected_id}")
with st.spinner("Fetching events..."):
    events = events_for_id(selected_id, start, end)

if not events:
    st.info("No events for that ID in this window.")
else:
    # Build a clean table with only msg, plus timestamp & stream
    rows = []
    for r in events:
        msg_only = parse_msg_only(r.get("@message", ""))
        rows.append({
            "timestamp(UTC)": r.get("@timestamp"),
            "logGroup": r.get("@logGroup"),
            "logStream": r.get("@logStream"),
            "msg": msg_only,
            "openInCloudWatch": cw_stream_link(region, r.get("@logGroup",""), r.get("@logStream",""), r.get("@timestamp",""))
        })
    details_df = pd.DataFrame(rows)
    # Render as a table; we’ll show the link as a clickable Markdown list below for clarity
    st.dataframe(details_df[["timestamp(UTC)","logStream","msg"]], use_container_width=True, hide_index=True)

    # Optional: per-row links
    with st.expander("Open these rows in CloudWatch"):
        for r in rows:
            if r["openInCloudWatch"]:
                st.markdown(f"- `{r['timestamp(UTC)']}` · `{r['logStream']}` → [Open in CloudWatch]({r['openInCloudWatch']})")

if auto:
    time.sleep(30)
    st.experimental_rerun()

