# import os, time, json, re
# from datetime import datetime, timedelta, timezone
# from typing import List, Dict, Any
# import boto3
# import streamlit as st
# import pandas as pd

# # --------- Config (env or defaults) ----------
# AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
# LOG_GROUPS = [g.strip() for g in os.getenv("LOG_GROUPS", "/ecs/stage-agent-backend-task").split(",") if g.strip()]
# DEFAULT_MINUTES = int(os.getenv("DEFAULT_MINUTES", "60"))  # last 60 mins by default
# LAMBDA_GROUP = os.getenv("LAMBDA_GROUP")
# if LAMBDA_GROUP:
#     LOG_GROUPS.append(LAMBDA_GROUP)

# boto_session = boto3.Session(region_name=AWS_REGION)
# logs = boto_session.client("logs")

# # --------- Helpers ----------
# def run_insights(query: str, start: datetime, end: datetime, log_groups: List[str], limit: int = 10000) -> List[Dict[str, str]]:
#     qid = logs.start_query(
#         logGroupNames=log_groups,
#         startTime=int(start.timestamp()),
#         endTime=int(end.timestamp()),
#         queryString=query,
#         limit=limit,
#     )["queryId"]
#     while True:
#         out = logs.get_query_results(queryId=qid)
#         status = out.get("status")
#         if status in ("Complete", "Failed", "Cancelled"):
#             break
#         time.sleep(1)
#     if status != "Complete":
#         st.warning(f"Query did not complete: {status}")
#         return []
#     rows = []
#     for r in out.get("results", []):
#         row = {}
#         for c in r:
#             row[c.get("field")] = c.get("value")
#         rows.append(row)
#     return rows

# def summary_rows(start: datetime, end: datetime) -> List[Dict[str, Any]]:
#     """group by correlationId, pull latest sender/to/logStream/message."""
#     query = r"""
# fields @timestamp, @logStream, @message
# | parse @message /["']correlationId["']\s*:\s*["'](?<cid_json>[^"']+)["']/
# | parse @message /correlationId\s*[:=]\s*(?<cid_inline>[^,"'\s}}]+)/
# | parse @message /["']senderEmail["']\s*:\s*["'](?<se_json>[^"']+)["']/
# | parse @message /senderEmail\s*[:=]\s*(?<se_inline>[^,"'\s}}]+)/
# | parse @message /["']toEmail["']\s*:\s*["'](?<te_json>[^"']+)["']/
# | parse @message /toEmail\s*[:=]\s*(?<te_inline>[^,"'\s}}]+)/
# | fields coalesce(cid_json, cid_inline) as correlationId,
#          coalesce(se_json, se_inline) as senderEmail_v,
#          coalesce(te_json, te_inline) as toEmail_v,
#          @timestamp, @logStream, @message
# | filter ispresent(correlationId)
# | stats min(@timestamp) as firstTs,
#         latest(senderEmail_v) as senderEmail,
#         latest(toEmail_v) as toEmail,
#         latest(@logStream) as logStream,
#         latest(@message) as message
#   by correlationId
# | sort correlationId desc
# | limit 500
# """.strip()
#     return run_insights(query, start, end, LOG_GROUPS)

# def events_for_id(correlation_id: str, start: datetime, end: datetime) -> List[Dict[str, str]]:
#     """All raw events for the chosen correlationId (across groups)."""
#     query = r"""
# fields @timestamp, @logGroup, @logStream, @message
# | parse @message /["']correlationId["']\s*:\s*["'](?<cid_json>[^"']+)["']/
# | parse @message /correlationId\s*[:=]\s*(?<cid_inline>[^,"'\s}}]+)/
# | fields coalesce(cid_json, cid_inline) as correlationId, @timestamp, @logGroup, @logStream, @message
# | filter correlationId = "{}"
# | sort @timestamp asc
# | limit 5000
# """.strip().format(correlation_id)
#     return run_insights(query, start, end, LOG_GROUPS)

# # Extract only the "msg" value from a log line
# def parse_msg_only(message: str) -> str:
#     if not message:
#         return ""
#     s = message.strip()
#     # If the line has a prefix like "INFO:root:{"..."}", drop everything before the first '{'
#     brace = s.find("{")
#     if brace != -1:
#         json_part = s[brace:]
#         try:
#             obj = json.loads(json_part)
#             if isinstance(obj, dict) and "msg" in obj:
#                 return str(obj.get("msg", ""))
#         except Exception:
#             pass  # fall through to patterns
#     # Try to find msg in a JSON-ish substring: "msg":"...".
#     m = re.search(r'"msg"\s*:\s*"([^"]+)"', s)
#     if m:
#         return m.group(1)
#     # Try inline style: msg: something, (until comma or EOL)
#     m2 = re.search(r'\bmsg\s*:\s*([^,}]+)', s)
#     if m2:
#         return m2.group(1).strip()
#     # Fallback: return the whole message
#     return s

# def cw_stream_link(region: str, group: str, stream: str, ts_iso: str) -> str:
#     try:
#         ts = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
#     except Exception:
#         return ""
#     start = int((ts - timedelta(minutes=5)).timestamp() * 1000)
#     end   = int((ts + timedelta(minutes=5)).timestamp() * 1000)
#     return (
#         f"https://{region}.console.aws.amazon.com/cloudwatch/home"
#         f"?region={region}#logsV2:log-groups/log-group/{group.replace('/','$252F')}"
#         f"/log-events/{stream.replace('/','$252F')}?start={start}&end={end}"
#     )

# # --------- UI ----------
# st.set_page_config(page_title="8vdx – Flow Logs", layout="wide")
# st.title("8vdx · Flow Logs (CloudWatch)")

# # Controls
# c1, c2, c3, c4 = st.columns([1,1,1,1])
# with c1:
#     time_unit = st.selectbox("Time unit", ["Minutes", "Hours", "Days"], index=0)
# with c2:
#     if time_unit == "Minutes":
#         time_value = st.slider("Time window", min_value=5, max_value=1440, value=DEFAULT_MINUTES, step=5)
#         minutes = time_value
#     elif time_unit == "Hours":
#         time_value = st.slider("Time window", min_value=1, max_value=168, value=DEFAULT_MINUTES//60, step=1)
#         minutes = time_value * 60
#     else:  # Days
#         time_value = st.slider("Time window", min_value=1, max_value=30, value=DEFAULT_MINUTES//1440, step=1)
#         minutes = time_value * 24 * 60
# with c3:
#     auto = st.checkbox("Auto-refresh every 30s", value=False)
# with c4:
#     region = st.text_input("AWS Region", AWS_REGION)# Display time range info
# time_display = f"{time_value} {time_unit.lower()}"
# if time_unit == "Minutes" and time_value >= 60:
#     hours = time_value // 60
#     remaining_minutes = time_value % 60
#     if remaining_minutes > 0:
#         time_display += f" ({hours}h {remaining_minutes}m)"
#     else:
#         time_display += f" ({hours}h)"
# elif time_unit == "Hours" and time_value >= 24:
#     days = time_value // 24
#     remaining_hours = time_value % 24
#     if remaining_hours > 0:
#         time_display += f" ({days}d {remaining_hours}h)"
#     else:
#         time_display += f" ({days}d)"

# st.caption(f"Reading from log groups: {', '.join(LOG_GROUPS)} | Time window: {time_display}")
# end = datetime.now(timezone.utc)
# start = end - timedelta(minutes=minutes)

# # ---------- BOX 1: Summary (like TXT1) ----------
# st.subheader("Flows (grouped by correlationId)")
# with st.spinner("Loading flows..."):
#     flows = summary_rows(start, end)

# if not flows:
#     st.info("No flows found in the selected time window.")
#     st.stop()

# # Build summary DataFrame
# summary_df = pd.DataFrame(flows)

# # Define the columns we want to display, checking if they exist
# display_columns = ["correlationId"]
# optional_columns = ["senderEmail", "toEmail", "logStream", "message", "firstTs"]

# # Add optional columns if they exist in the data
# for col in optional_columns:
#     if col in summary_df.columns:
#         display_columns.append(col)

# # Select only the columns that exist
# summary_df = summary_df[display_columns]

# # Rename firstTs if it exists
# if "firstTs" in summary_df.columns:
#     summary_df = summary_df.rename(columns={"firstTs": "startTime(UTC)"})

# # Show summary table with clickable correlation IDs
# st.markdown("**Click on any correlation ID in the table below to view its logs:**")
# selected_rows = st.dataframe(
#     summary_df, 
#     use_container_width=True, 
#     hide_index=True,
#     on_select="rerun",
#     selection_mode="single-row"
# )

# # Get selected correlation ID
# if selected_rows.selection.rows:
#     selected_row_index = selected_rows.selection.rows[0]
#     selected_id = summary_df.iloc[selected_row_index]["correlationId"]
#     st.success(f"Selected correlation ID: **{selected_id}**")
# else:
#     # Default to first row if nothing is selected
#     selected_id = summary_df.iloc[0]["correlationId"]
#     st.info(f"Showing logs for: **{selected_id}** (click on a row to select a different correlation ID)")

# # ---------- BOX 2: Details (msg only) ----------
# st.subheader(f"Logs (msg only) for correlationId: {selected_id}")
# with st.spinner("Fetching events..."):
#     events = events_for_id(selected_id, start, end)

# if not events:
#     st.info("No events for that ID in this window.")
# else:
#     # Build a clean table with only msg, plus timestamp & stream
#     rows = []
#     for r in events:
#         msg_only = parse_msg_only(r.get("@message", ""))
#         rows.append({
#             "timestamp(UTC)": r.get("@timestamp"),
#             "logGroup": r.get("@logGroup"),
#             "logStream": r.get("@logStream"),
#             "msg": msg_only,
#             "openInCloudWatch": cw_stream_link(region, r.get("@logGroup",""), r.get("@logStream",""), r.get("@timestamp",""))
#         })
#     details_df = pd.DataFrame(rows)
#     # Render as a table; we’ll show the link as a clickable Markdown list below for clarity
#     st.dataframe(details_df[["timestamp(UTC)","logStream","msg"]], use_container_width=True, hide_index=True)

#     # Optional: per-row links
#     with st.expander("Open these rows in CloudWatch"):
#         for r in rows:
#             if r["openInCloudWatch"]:
#                 st.markdown(f"- `{r['timestamp(UTC)']}` · `{r['logStream']}` → [Open in CloudWatch]({r['openInCloudWatch']})")

# if auto:
#     time.sleep(30)
#     st.rerun()

# app.py
import os, time, json, re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import streamlit as st

# --- Helpful error if boto3 isn't installed on Streamlit Cloud ---
try:
    import boto3
except ModuleNotFoundError as e:
    st.error(
        "Missing dependency: **boto3**\n\n"
        "Add this line to your `requirements.txt` at the repo root and redeploy:\n\n"
        "```\n"
        "boto3>=1.34\n"
        "```\n"
        "If you're on Streamlit Cloud, push the change and reboot the app."
    )
    st.stop()

import pandas as pd

# =========================
# Config: Secrets & Env
# =========================
# You can set secrets in Streamlit Cloud like:
# [aws]
# aws_access_key_id = "AKIA..."
# aws_secret_access_key = "..."
# region_name = "us-east-1"
#
# [cloudwatch]
# log_groups = "/ecs/stage-agent-backend-task,/aws/lambda/my-func"  # comma-separated
#
# [environment]       # optional: env-style variables you want available
# DEFAULT_MINUTES = "60"
# LAMBDA_GROUP = "/aws/lambda/another-func"

aws_secrets = st.secrets.get("aws", {})
cw_secrets = st.secrets.get("cloudwatch", {})
env_secrets = st.secrets.get("environment", {})

# Make environment-style secrets available to code that expects env vars
if isinstance(env_secrets, dict) and env_secrets:
    os.environ.update({k: str(v) for k, v in env_secrets.items()})

# Region (secrets -> env -> default)
AWS_REGION = (
    aws_secrets.get("region_name")
    or os.getenv("AWS_REGION")
    or "us-east-1"
)

# Log groups (secrets -> env -> sensible default)
log_groups_str = (
    cw_secrets.get("log_groups")
    or os.getenv("LOG_GROUPS")
    or "/ecs/stage-agent-backend-task"
)
LOG_GROUPS: List[str] = [g.strip() for g in log_groups_str.split(",") if g.strip()]

# Optional: add LAMBDA_GROUP if provided (secrets/env)
LAMBDA_GROUP = os.getenv("LAMBDA_GROUP") or env_secrets.get("LAMBDA_GROUP")
if LAMBDA_GROUP:
    LOG_GROUPS.append(LAMBDA_GROUP)

# Time window default (env or secrets or fallback)
DEFAULT_MINUTES = int(
    os.getenv("DEFAULT_MINUTES")
    or env_secrets.get("DEFAULT_MINUTES", "60")
)

# =========================
# Boto3 session
# =========================
# Prefer explicit secrets if provided; otherwise rely on instance role / env / profile.
def make_boto_session(region_name: str) -> boto3.session.Session:
    access_key = aws_secrets.get("aws_access_key_id")
    secret_key = aws_secrets.get("aws_secret_access_key")
    session_token = aws_secrets.get("aws_session_token")  # optional

    if access_key and secret_key:
        return boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region_name,
        )
    # Fallback to default provider chain (env vars, role, etc.)
    return boto3.session.Session(region_name=region_name)

boto_session = make_boto_session(AWS_REGION)
logs = boto_session.client("logs")

# =========================
# Helpers
# =========================

def _sleep_backoff(attempt: int) -> None:
    # small exponential backoff with cap
    time.sleep(min(1 * (2 ** attempt), 5))

@st.cache_data(show_spinner=False, ttl=15)
def run_insights_cached(query: str, start_epoch: int, end_epoch: int, log_groups: tuple, limit: int) -> List[Dict[str, str]]:
    """
    Cached wrapper to avoid hammering Insights when the UI re-runs.
    Cache key is based on scalar args; lists must be tuples.
    """
    return _run_insights_uncached(query, start_epoch, end_epoch, list(log_groups), limit)

def _run_insights_uncached(query: str, start_epoch: int, end_epoch: int, log_groups: List[str], limit: int = 10000) -> List[Dict[str, str]]:
    if not log_groups:
        st.warning("No CloudWatch log groups configured.")
        return []

    # Start query
    attempts = 0
    while True:
        try:
            resp = logs.start_query(
                logGroupNames=log_groups,
                startTime=start_epoch,
                endTime=end_epoch,
                queryString=query,
                limit=limit,
            )
            qid = resp["queryId"]
            break
        except logs.exceptions.LimitExceededException:
            # CW Insights concurrency/rate limit—retry
            _sleep_backoff(attempts)
            attempts += 1
        except Exception as e:
            st.error(f"Failed to start query: {e}")
            return []

    # Poll until complete
    status = "Running"
    for _ in range(240):  # up to ~240s
        try:
            out = logs.get_query_results(queryId=qid)
            status = out.get("status")
            if status in ("Complete", "Failed", "Cancelled"):
                break
        except Exception as e:
            st.warning(f"get_query_results error: {e}")
        time.sleep(1)

    if status != "Complete":
        st.warning(f"Query did not complete: {status}")
        return []

    rows: List[Dict[str, str]] = []
    for r in out.get("results", []):
        row: Dict[str, str] = {}
        for c in r:
            row[c.get("field")] = c.get("value")
        rows.append(row)
    return rows

def run_insights(query: str, start: datetime, end: datetime, log_groups: List[str], limit: int = 10000) -> List[Dict[str, str]]:
    return run_insights_cached(
        query=query,
        start_epoch=int(start.timestamp()),
        end_epoch=int(end.timestamp()),
        log_groups=tuple(log_groups),
        limit=limit,
    )

def summary_rows(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Group by correlationId, pull latest sender/to/logStream/message."""
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
""".strip().format(correlation_id.replace('"', '\\"'))
    return run_insights(query, start, end, LOG_GROUPS)

# Extract only the "msg" value from a log line
def parse_msg_only(message: str) -> str:
    if not message:
        return ""
    s = message.strip()
    # Drop prefix before first '{' if present
    brace = s.find("{")
    if brace != -1:
        json_part = s[brace:]
        try:
            obj = json.loads(json_part)
            if isinstance(obj, dict) and "msg" in obj:
                return str(obj.get("msg", ""))
        except Exception:
            pass  # fall through
    m = re.search(r'"msg"\s*:\s*"([^"]+)"', s)
    if m:
        return m.group(1)
    m2 = re.search(r'\bmsg\s*:\s*([^,}]+)', s)
    if m2:
        return m2.group(1).strip()
    return s

def cw_stream_link(region: str, group: str, stream: str, ts_iso: str) -> str:
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    except Exception:
        return ""
    start_ms = int((ts - timedelta(minutes=5)).timestamp() * 1000)
    end_ms   = int((ts + timedelta(minutes=5)).timestamp() * 1000)
    # minimal encoding for '/' expected by CW console deep links
    enc = lambda s: s.replace("/", "$252F")
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups/log-group/{enc(group)}"
        f"/log-events/{enc(stream)}?start={start_ms}&end={end_ms}"
    )

# =========================
# UI
# =========================
st.set_page_config(page_title="8vdx – Flow Logs", layout="wide")
st.title("8vdx · Flow Logs (CloudWatch)")

if not LOG_GROUPS:
    st.stop()

# Controls
c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
with c1:
    time_unit = st.selectbox("Time unit", ["Minutes", "Hours", "Days"], index=0)
with c2:
    if time_unit == "Minutes":
        time_value = st.slider("Time window", min_value=5, max_value=1440, value=DEFAULT_MINUTES, step=5)
        minutes = time_value
    elif time_unit == "Hours":
        time_value = st.slider("Time window", min_value=1, max_value=168, value=max(1, DEFAULT_MINUTES // 60), step=1)
        minutes = time_value * 60
    else:  # Days
        time_value = st.slider("Time window", min_value=1, max_value=30, value=max(1, DEFAULT_MINUTES // 1440), step=1)
        minutes = time_value * 24 * 60
with c3:
    auto = st.checkbox("Auto-refresh every 30s", value=False)
with c4:
    region = st.text_input("AWS Region", AWS_REGION)

# Display time range info
time_display = f"{time_value} {time_unit.lower()}"
if time_unit == "Minutes" and time_value >= 60:
    hours = time_value // 60
    remaining_minutes = time_value % 60
    time_display += f" ({hours}h{f' {remaining_minutes}m' if remaining_minutes else ''})"
elif time_unit == "Hours" and time_value >= 24:
    days = time_value // 24
    remaining_hours = time_value % 24
    time_display += f" ({days}d{f' {remaining_hours}h' if remaining_hours else ''})"

st.caption(f"Reading from log groups: {', '.join(LOG_GROUPS)} | Time window: {time_display}")

end = datetime.now(timezone.utc)
start = end - timedelta(minutes=minutes)

# ---------- BOX 1: Summary ----------
st.subheader("Flows (grouped by correlationId)")
with st.spinner("Loading flows..."):
    flows = summary_rows(start, end)

if not flows:
    st.info("No flows found in the selected time window.")
    if auto:
        time.sleep(30)
        st.rerun()
    st.stop()

summary_df = pd.DataFrame(flows)

# Define the columns we want to display, checking if they exist
display_columns = ["correlationId"]
optional_columns = ["senderEmail", "toEmail", "logStream", "message", "firstTs"]
for col in optional_columns:
    if col in summary_df.columns:
        display_columns.append(col)

summary_df = summary_df[display_columns]
if "firstTs" in summary_df.columns:
    summary_df = summary_df.rename(columns={"firstTs": "startTime(UTC)"})

st.markdown("**Click on any correlation ID in the table below to view its logs:**")

# Use st.dataframe with selection for single-row selection
selected_row_index = 0
try:
    df_widget = st.dataframe(
        summary_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="summary_df_select",
    )
    if getattr(df_widget, "selection", None) and df_widget.selection.rows:
        selected_row_index = df_widget.selection.rows[0]
except Exception:
    pass

selected_id = summary_df.iloc[selected_row_index]["correlationId"]
st.info(f"Showing logs for: **{selected_id}** (click a row to select a different correlation ID)")

# ---------- BOX 2: Details (msg only) ----------
st.subheader(f"Logs (msg only) for correlationId: {selected_id}")
with st.spinner("Fetching events..."):
    events = events_for_id(selected_id, start, end)

if not events:
    st.info("No events for that ID in this window.")
else:
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
    st.dataframe(details_df[["timestamp(UTC)", "logStream", "msg"]], use_container_width=True, hide_index=True)

    with st.expander("Open these rows in CloudWatch"):
        for r in rows:
            if r["openInCloudWatch"]:
                st.markdown(f"- `{r['timestamp(UTC)']}` · `{r['logStream']}` → [Open in CloudWatch]({r['openInCloudWatch']})")

if auto:
    time.sleep(30)
    st.rerun()
