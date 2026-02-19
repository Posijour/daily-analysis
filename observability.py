from datetime import datetime


def _format_module_name(module_name: str) -> str:
    return module_name.replace("_", " ").capitalize()


def _render_event_message(event: str, fields: dict) -> str:
    if event == "daily.started":
        return (
            "Daily analysis started. Window UTC: "
            f"{fields.get('window_start')} -> {fields.get('window_end')}"
        )

    if event == "daily.module.ok":
        module_label = _format_module_name(str(fields.get("module", "module")))
        return f"{module_label} daily completed."

    if event == "daily.module.failed":
        module_label = _format_module_name(str(fields.get("module", "module")))
        return f"{module_label} daily failed: {fields.get('error')}"

    if event == "daily.status_sync.failed":
        return f"Daily job status sync failed: {fields.get('error')}"

    if event == "daily.finished":
        return f"Daily analysis finished with status: {fields.get('status')}"

    if event == "daily.skipped":
        return f"Daily analysis skipped: {fields.get('reason')}"

    details = ", ".join(f"{key}={value}" for key, value in fields.items())
    return f"{event}: {details}" if details else event


def log_event(event: str, **fields):
    timestamp = datetime.now().strftime("%I:%M:%S %p")
    message = _render_event_message(event, fields)
    print(f"{timestamp}  {message}")
