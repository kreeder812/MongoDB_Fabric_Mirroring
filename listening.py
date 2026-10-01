import pymongo
import pandas as pd
import time
import logging
import os
import pickle
from threading import Thread
import threading
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
    TEMP_PREFIX_DURING_INIT,
    DATA_FILES_PATH,
    DELTA_SYNC_CACHE_PARQUET_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
# added the two new files to save the initial sync status and last parquet file number
    INIT_SYNC_STATUS_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
    DTYPE_KEY,
    TYPE_KEY,
)
from utils import to_string, get_parquet_full_path_filename, get_temp_parquet_full_path_filename, get_table_dir
from push_file_to_lz import push_file_to_lz
#from flags import get_init_flag
from init_sync import init_sync
import schemas
import schema_utils
from file_utils import FileType, read_from_file, write_to_file


# ---------------------------------------------------------------------------
# Listener resilience: heartbeat watchdog + thread exception logging
# ---------------------------------------------------------------------------
# Every listener loop iteration (at least once per max_await_time_ms = 20s, even
# with no changes) records a heartbeat. If any listener goes silent for longer
# than LISTENER_STALL_SECONDS - because its thread died or hung - the watchdog
# logs CRITICAL and exits the process so App Service starts a fresh container,
# which resumes every collection from its persisted resume token.
LISTENER_STALL_SECONDS = float(os.getenv("LISTENER_STALL_SECONDS", "600"))
WATCHDOG_INTERVAL_SECONDS = 60
MAX_POISON_RETRIES = 3          # same change failing this many times -> skip it
MAX_ERROR_BACKOFF_SECONDS = 300  # must stay well under LISTENER_STALL_SECONDS

_heartbeats: dict = {}
_hb_lock = threading.Lock()
_watchdog_started = False


def _beat(collection_name: str):
    with _hb_lock:
        _heartbeats[collection_name] = time.time()


def _watchdog():
    log = logging.getLogger(f"{__name__}[watchdog]")
    log.info(
        "listener watchdog started (stall threshold %.0fs, check every %ds)",
        LISTENER_STALL_SECONDS,
        WATCHDOG_INTERVAL_SECONDS,
    )
    while True:
        time.sleep(WATCHDOG_INTERVAL_SECONDS)
        now = time.time()
        with _hb_lock:
            snapshot = dict(_heartbeats)
        stalled = {
            coll: round(now - ts)
            for coll, ts in snapshot.items()
            if now - ts > LISTENER_STALL_SECONDS
        }
        if stalled:
            log.critical(
                "LISTENER STALL: no heartbeat (seconds since last) %s; "
                "exiting process so App Service restarts the container",
                stalled,
            )
            # Give handlers (incl. the App Insights exporter) a chance to flush.
            try:
                logging.shutdown()
            finally:
                time.sleep(5)
                os._exit(1)


def _start_watchdog_once():
    global _watchdog_started
    with _hb_lock:
        if _watchdog_started:
            return
        _watchdog_started = True
    Thread(target=_watchdog, name="listener-watchdog", daemon=True).start()


def _thread_excepthook(args):
    # Default behavior only prints to stderr, which never reaches App Insights.
    if args.exc_type is SystemExit:
        return
    logging.getLogger(__name__).critical(
        "UNCAUGHT exception in thread %s",
        args.thread.name if args.thread else "<unknown>",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


threading.excepthook = _thread_excepthook


def _reload_persisted_resume_token(collection_name: str, logger):
    """Rewind to the last token written alongside a successful LZ push.

    Used after a non-pymongo failure: the in-memory token has already advanced
    past changes that were sitting in the (now discarded) unflushed batch, so
    reusing it would silently drop those changes.
    """
    while True:
        try:
            return read_from_file(
                collection_name, DELTA_SYNC_RESUME_TOKEN_FILE_NAME, FileType.PICKLE
            )
        except Exception:
            # If the LZ is unreachable for longer than the stall threshold the
            # watchdog will restart the container, which is fine.
            logger.error(
                "could not read persisted resume token for %s; retrying in 30s",
                collection_name,
                exc_info=True,
            )
            time.sleep(30)


def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    threading.current_thread().name = f"listener-{collection_name}"
    _beat(collection_name)
    _start_watchdog_once()

    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug(f"db_name={db_name}")
    logger.debug(f"collection={collection_name}")
    # moved listening method so that it is called after the env variables are loaded
    time_threshold_in_sec = float(os.getenv("TIME_THRESHOLD_IN_SEC"))
    post_init_flush_done = False

    # table_dir = get_table_dir(collection_name) #Never used
    resume_token = read_from_file(
        collection_name, DELTA_SYNC_RESUME_TOKEN_FILE_NAME, FileType.PICKLE
    )
    if resume_token:
        logger.info(
            f"interrupted incremental sync detected, continuing with resume_token={resume_token}"
        )

    #MongoDB connection and data info
    client = pymongo.MongoClient(
        os.getenv("MONGO_CONN_STR"),
        # 0 or None = no driver‑side socket timeout
        socketTimeoutMS=None,
        # (optionally) set a sane connect timeout instead of a read timeout
        connectTimeoutMS=20000,
    )
    db = client[db_name]
    collection = db[collection_name]

    #cursor = collection.watch(full_document="updateLookup", resume_after=resume_token, max_await_time_ms=20000)

    # use df  - enables variable schemas
    # and consistent as resume_token is updated when file is pushed to LZ
    accumulative_df: pd.DataFrame = None
    init_sync_stat_flag = None
    last_sync_time: float | None = None

    # Failure bookkeeping for non-pymongo errors
    failure_counts: dict = {}   # change _id (str) -> times it has failed in "process"
    poison_skip: set = set()    # change _ids to skip on replay
    error_backoff = 2

    # start init sync after we get cursor from Change Stream
    Thread(target=init_sync, args=(collection_name,)).start()
    logger.info(f"start listening to change stream for collection {collection_name}")
    
    # New main loop logic
    while True:
        # Build watch options each time we open a new stream
        watch_kwargs = dict(
            full_document="updateLookup",
            max_await_time_ms=20000,
        )
        if resume_token:
            watch_kwargs["resume_after"] = resume_token

        change = None
        phase = "open"  # open | process | flush - tells the handler what failed

        try:
            with collection.watch(**watch_kwargs) as stream:
                logger.info(
                    "opened change stream for %s with resume_token=%s",
                    collection_name,
                    resume_token,
                )
                last_action_time = datetime.now()
                # Use try_next so we can flush on time threshold even without new events
                while True:
                    before = time.time()
                    change = stream.try_next()
                    after = time.time()
                    _beat(collection_name)

                    if change is None:
                        if (datetime.now() - last_action_time >= timedelta(minutes=5)):
                            logger.info("no change; try_next() round-trip took %.3fs", after - before)
                            last_action_time = datetime.now()
                        # No new events in this await interval; consider time-based flush
                        if (
                            accumulative_df is not None
                            and init_sync_stat_flag == "Y"
                            and last_sync_time is not None
                        ):
                            phase = "flush"
                            accumulative_df, last_sync_time = process_accumulative_df(
                                accumulative_df,
                                collection_name,
                                init_sync_stat_flag,
                                last_sync_time,
                                time_threshold_in_sec,
                                resume_token,
                                logger,
                            )
                        continue

                    # ---- We have a real change document here ----
                    phase = "process"

                    change_key = str(change["_id"])
                    if change_key in poison_skip:
                        logger.error(
                            "SKIPPING previously failing change %s (op=%s, documentKey=%s)",
                            change_key,
                            change.get("operationType"),
                            change.get("documentKey"),
                        )
                        resume_token = change["_id"]
                        continue

                    if init_sync_stat_flag != "Y":
                        init_sync_stat_flag = read_from_file(
                            collection_name,
                            INIT_SYNC_STATUS_FILE_NAME,
                            FileType.PICKLE,
                        )

                    if init_sync_stat_flag == "Y" and not post_init_flush_done:
                        __post_init_flush(collection_name, logger)
                        post_init_flush_done = True

                    logger.debug("original change from Change Stream:")
                    logger.debug(change)

                    operationType = change["operationType"]
                    if operationType not in CHANGE_STREAM_OPERATION_MAP:
                        logger.error("ERROR: unsupported operation found: %s", operationType)
                        continue

                    if operationType == "delete":
                        doc: dict = change["documentKey"]
                    else:
                        doc: dict = change["fullDocument"]

                    df = pd.DataFrame([doc])

                    # Always update resume_token on every processed change
                    resume_token = change["_id"]
                    logger.debug("resume_token: %s", resume_token)

                    schema_utils.process_dataframe(collection_name, df)

                    if init_sync_stat_flag != "Y":
                        logger.debug(
                            "collection %s still initializing, use UPSERT instead of INSERT",
                            collection_name,
                        )
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT[
                            operationType
                        ]
                    else:
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]

                    df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])

                    # Merge into accumulative_df until batch size/time threshold
                    if accumulative_df is not None:
                        accumulative_df = pd.concat(
                            [accumulative_df, df], ignore_index=True
                        )
                        logger.info("concat accumulative_df result:")
                        logger.info(accumulative_df)
                    else:
                        logger.info("df created")
                        accumulative_df = df
                        last_sync_time = time.time()
                        logger.info(
                            "last_sync_time when first record added: %s", last_sync_time
                        )

                    phase = "flush"
                    accumulative_df, last_sync_time = process_accumulative_df(
                        accumulative_df,
                        collection_name,
                        init_sync_stat_flag,
                        last_sync_time,
                        time_threshold_in_sec,
                        resume_token,
                        logger,
                    )
                    error_backoff = 2  # a full cycle succeeded

                # End inner while True

        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.OperationFailure,
            # was `pymongo.Error`, which does not exist in pymongo 4.x. Evaluating it
            # raised AttributeError whenever ANY exception reached this handler,
            # which escaped listening() and silently killed the thread.
            PyMongoError,
        ) as exc:
            _beat(collection_name)
            # Detect non-resumable ChangeStreamHistoryLost / stale resume token.
            is_non_resumable = (
                isinstance(exc, pymongo.errors.OperationFailure)
                and (
                    exc.code == 286  # ChangeStreamHistoryLost
                    or exc.has_error_label("NonResumableChangeStreamError")
                )
            )

            if is_non_resumable:
                logger.error(
                    "Non-resumable Change Stream error (ChangeStreamHistoryLost) for collection %s: %s. "
                    "Clearing resume token and restarting from latest position.",
                    collection_name,
                    exc,
                    exc_info=True,
                )

                # Drop the bad resume token so the next loop does *not* send resume_after.
                resume_token = None

                # Persist that change so a restart doesn't reuse the stale token.
                write_to_file(
                    None,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )

                # Optional: clear any in-memory batch since we've lost continuity anyway.
                accumulative_df = None
                last_sync_time = None
            else:
                # Resumable errors: keep the last known resume_token.
                logger.warning(
                    "Resumable change stream error for collection %s: %s; "
                    "will reopen with last resume_token=%s",
                    collection_name,
                    exc,
                    resume_token,
                    exc_info=True,
                )

            # Outer while True will rebuild watch_kwargs and reopen.
            # Slight backoff to avoid tight reconnect loop
            time.sleep(2)
            continue

        except Exception as exc:
            # Anything else - schema coercion, parquet write, LZ push, file I/O.
            # Previously these escaped listening() and silently killed the thread.
            _beat(collection_name)

            if phase == "process" and change is not None:
                key = str(change["_id"])
                failure_counts[key] = failure_counts.get(key, 0) + 1
                attempts = failure_counts[key]
                if attempts >= MAX_POISON_RETRIES:
                    poison_skip.add(key)
                    failure_counts.pop(key, None)
                    logger.error(
                        "change %s (op=%s, documentKey=%s) failed %d times while processing; "
                        "it will be SKIPPED on replay. That document may be stale in Fabric "
                        "until its next update.",
                        key,
                        change.get("operationType"),
                        change.get("documentKey"),
                        attempts,
                        exc_info=True,
                    )
                else:
                    logger.error(
                        "failed processing change %s (op=%s, documentKey=%s), attempt %d/%d; "
                        "replaying from last persisted resume token",
                        key,
                        change.get("operationType"),
                        change.get("documentKey"),
                        attempts,
                        MAX_POISON_RETRIES,
                        exc_info=True,
                    )
            else:
                # Flush / LZ failures are batch- or infrastructure-level; never skip,
                # just keep retrying loudly.
                logger.error(
                    "failure in %s phase for %s; discarding unflushed batch and replaying "
                    "from last persisted resume token (next retry in %ds)",
                    phase,
                    collection_name,
                    error_backoff,
                    exc_info=True,
                )

            # Discard the unflushed batch and rewind so nothing is lost.
            accumulative_df = None
            last_sync_time = None
            resume_token = _reload_persisted_resume_token(collection_name, logger)

            time.sleep(error_backoff)
            error_backoff = min(error_backoff * 2, MAX_ERROR_BACKOFF_SECONDS)
            continue

        # If we ever exit the inner loop *without* an exception:
        # check stream.alive to see if the server closed the cursor.
        if not stream.alive:
            logger.warning(
                "change stream closed by server for collection %s; reopening with last resume_token=%s",
                collection_name,
                resume_token,
            )
            # Outer while True will reopen
            continue

##>> enhancement to check time elapsed even if no event comes - no waiting indefinitely for a change
def process_accumulative_df(accumulative_df, collection_name, init_sync_stat_flag, last_sync_time, time_threshold_in_sec, resume_token, logger):
    if not init_sync_stat_flag == "Y":
        if (accumulative_df is not None
            and (
                (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
            )
        ):
            prefix = TEMP_PREFIX_DURING_INIT
            parquet_full_path_filename = get_temp_parquet_full_path_filename(
                collection_name, prefix=prefix
            )
            logger.info(f"writing TEMP parquet file: {parquet_full_path_filename}")
            accumulative_df.to_parquet(parquet_full_path_filename)
            accumulative_df = None
    else:        
        if (accumulative_df is not None
        ):
            if(
                (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
                or ((time.time() - last_sync_time) >= time_threshold_in_sec)
            ):
                prefix = ""
                last_parquet_file_num = read_from_file(
                    collection_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
                )
                if not last_parquet_file_num:
                    last_parquet_file_num = 0

                parquet_full_path_filename = get_parquet_full_path_filename(collection_name, last_parquet_file_num)

                logger.info(f"writing parquet file: {parquet_full_path_filename}")
                # Convert any remaining Object column into String
                id_col = accumulative_df['_id']
                obj_cols = accumulative_df.select_dtypes(include=['object']).columns
                # Don't stringify columns with an explicit forced non-string type -
                # doing so undoes schema_utils.process_dataframe()'s coercion for any
                # batch containing a row that failed numeric conversion, which silently
                # re-breaks fields like subHaulerBaseAmount / divWeight4 right before write.
                forced_non_string_cols = {
                    col for col, (t, _) in schema_utils.FORCED_COLUMN_TYPES.items() if t is not str
                }
                obj_cols = [c for c in obj_cols if c not in forced_non_string_cols]
                accumulative_df[obj_cols] = accumulative_df[obj_cols].astype(str,errors="ignore")
                
                #  Restore the _id column
                accumulative_df['_id'] = id_col
                # Write the parquet file
                accumulative_df.to_parquet(parquet_full_path_filename)
                accumulative_df = None

                push_file_to_lz(parquet_full_path_filename, collection_name)
            #    resume_token = change["_id"]
                logger.info(f"writing resume_token into file: {resume_token}")
                write_to_file(
                    resume_token,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )
                last_parquet_file_num +=  1
                logger.info(f"writing last parquet number into file: {last_parquet_file_num}")
                write_to_file(
                    last_parquet_file_num,
                    collection_name,
                    LAST_PARQUET_FILE_NUMBER,
                    FileType.PICKLE,
            )
    return accumulative_df, last_sync_time

def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = get_table_dir(table_name)
    if not os.path.exists(table_dir):
        return
    temp_parquet_filename_list = sorted(
        [
            filename
            for filename in os.listdir(table_dir)
            if os.path.splitext(filename)[1] == ".parquet"
            and os.path.splitext(filename)[0].startswith(TEMP_PREFIX_DURING_INIT)
        ]
    )
    for temp_parquet_filename in temp_parquet_filename_list:
        temp_parquet_full_path = os.path.join(table_dir, temp_parquet_filename)
        # changed to get last parquet file number from LZ for resilience
        #new_parquet_full_path = get_parquet_full_path_filename(table_name)
        last_parquet_file_num = read_from_file(
            table_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
        )
        if not last_parquet_file_num:
            last_parquet_file_num = 0
        new_parquet_full_path = get_parquet_full_path_filename(table_name, last_parquet_file_num)   
        logger.debug("renaming temp parquet file")
        logger.debug(f"old name: {temp_parquet_full_path}")
        logger.debug(f"new name: {new_parquet_full_path}")
        logger.info(
            f"renaming parquet file from {temp_parquet_full_path} to {new_parquet_full_path}"
        )
        os.rename(temp_parquet_full_path, new_parquet_full_path)
        push_file_to_lz(new_parquet_full_path, table_name)
        # write last parquet file number to file
        last_parquet_file_num +=  1
        logger.info(f"writing last parquet number into file: {last_parquet_file_num}")
        write_to_file(
            last_parquet_file_num,
            table_name,
            LAST_PARQUET_FILE_NUMBER,
            FileType.PICKLE,
        )
