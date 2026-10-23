import logging

logger = logging.getLogger(__name__)


def log_task_failure(context) -> None:
    task_instance = context["task_instance"]

    logger.error(
        "Task failed | dag_id=%s | task_id=%s | run_id=%s | try_number=%s | logical_date=%s | exception=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        task_instance.try_number,
        context.get("logical_date"),
        context.get("exception"),
    )


def log_task_retry(context) -> None:
    task_instance = context["task_instance"]

    logger.warning(
        "Task retry scheduled | dag_id=%s | task_id=%s | run_id=%s | try_number=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        task_instance.try_number,
    )