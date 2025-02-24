import sys


def _format_time(seconds: int) -> str:
    time_parts = []
    remaining_seconds = seconds
    for unit, value in [("d", 86400), ("h", 3600), ("m", 60), ("s", 1)]:
        if duration := remaining_seconds // value:
            time_parts.append(f"{duration:.0f}{unit}")
            remaining_seconds %= value
    return " ".join(time_parts)


def log_progress(
    current_count: int, total_count: int, elapsed_time: float, task_title: str = None
):
    if total_count <= 0:
        return

    bar_len = 50
    percents = min(100.0 * current_count / total_count, 100.0)
    filled_len = int(round(bar_len * current_count / total_count))

    bar = "#" * filled_len + "∙" * (bar_len - filled_len)

    elapsed_str = _format_time(int(elapsed_time)) or "1s"
    eta_str = (
        _format_time(int((elapsed_time / max(percents, 0.1)) * (100 - percents)))
        if percents > 0
        else ""
    )

    sys.stdout.write(
        f"\r{task_title + ': ' if task_title else ''}{percents:.1f}% |{bar}| "
        f"elapsed: {elapsed_str}"
        f"{' eta: ' + eta_str if eta_str else ''} "
        f"({current_count}/{total_count if current_count < total_count else total_count})"
    )
    sys.stdout.flush()


import logging
import logging.handlers
import os
import shutil
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from atlassian import Confluence
from atlassian.errors import ApiPermissionError
from tenacity import (
    RetryCallState,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
    datefmt="[%X]",
)
logger = logging.getLogger("confluence")
logger.setLevel(logging.DEBUG)

confluence_url = os.getenv("CONFLUENCE_URL")
confluence_username = os.getenv("CONFLUENCE_USERNAME")
confluence_token = os.getenv("CONFLUENCE_TOKEN")
confluence_space_key = os.getenv("CONFLUENCE_SPACE_KEY")

confluence = Confluence(
    url=confluence_url,
    username=confluence_username,
    password=confluence_token,
)

def with_retry(max_retries=5, reraise: bool = False):
    def decorator(func):
        def log_retry_error(retry_state: RetryCallState):
            if retry_state.outcome.failed:
                ex = retry_state.outcome.exception()

                if isinstance(ex, ApiPermissionError):
                    logger.warning(
                        f"Failed to get page data. Space key: {space_key}. Error: {e}"
                    )
                    return None

                tb = traceback.format_exception(type(ex), ex, ex.__traceback__)
                logger.error(
                    f"Operation failed after {retry_state.attempt_number} attempts\n"
                    f"Function: {func.__name__}\n"
                    f"Arguments: {retry_state.args} | {retry_state.kwargs}\n"
                    f"Exception:\n{''.join(tb)}"
                )
                if reraise:
                    raise ex
            return None

        return retry(
            reraise=True,
            retry=retry_if_not_exception_type(ApiPermissionError),
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=5),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            retry_error_callback=log_retry_error,
        )(func)

    return decorator

def get_page_count(space_key: str) -> int:
    return with_retry()(confluence.cql)(
        f"space = '{space_key}' and type = 'page'", limit=1, expand=None
    ).get("totalSize")

def download_space(space_key: str, output_dir: str):
    try:
        start_time = time.perf_counter()
        pages_count = get_page_count(space_key)
        if not pages_count:
            logger.warning(f"No pages found in space {space_key}")
            return
        pages, start, limit = [], 0, 2
        while True:
            page_data = with_retry()(confluence.get_all_pages_from_space)(
                space_key, start=start, limit=limit, content_type="page", expand="space,body.view,version,container,modificationDate"
            )
            if not page_data:
                break
            pages.extend(page_data)
            start += len(page_data)
            elapsed_time = time.perf_counter() - start_time
            log_progress(len(pages), pages_count, elapsed_time, task_title=f"Downloading space: {space_key}")

        return pages
    
    except Exception as e:
        logger.exception(f"Failed to download space {space_key}. Error: {e}")



