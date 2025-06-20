import os
from airflow.sdk import asset


OBJECT_STORAGE_SYSTEM = os.getenv(
    "OBJECT_STORAGE_SYSTEM", default="file"
)
OBJECT_STORAGE_CONN_ID = os.getenv(
    "OBJECT_STORAGE_CONN_ID", default=None
)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter"
)

@asset(schedule="0 0 * * *")
def raw_zen_quotes() -> list[dict]:
    """
    Fetch quotes from Zen Quotes API
    """
    import requests
    url = "https://zenquotes.io/api/quotes/random"
    response = requests.get(url)
    return response.json()

@asset(schedule=[raw_zen_quotes])
def selected_quotes(context: dict) -> dict:
    """
    Transform the selected raw quotes
    """
    import numpy as np
    raw_zen_quotes_list = context["ti"].xcom_pull(
        dag_id="raw_zen_quotes",
        task_ids=["raw_zen_quotes"],
        key="return_value",
        include_prior_dates=True,
    )

    raw_zen_quotes = raw_zen_quotes_list[0]

    quotes_character_counts = [
        int(quote["c"]) for quote in raw_zen_quotes
    ] 
    median = np.median(quotes_character_counts)

    median_quote = min(
        raw_zen_quotes,
        key=lambda quote: abs(int(quote["c"]) - median),
    )

    raw_zen_quotes.pop(raw_zen_quotes.index(median_quote))

    short_quote = [
        quote for quote in raw_zen_quotes
        if int(quote["c"]) < median
    ][0]

    long_quote = [
        quote for quote in raw_zen_quotes
        if int(quote["c"]) > median
    ][0]

    return {
        "median_quote": median_quote,
        "short_quote": short_quote,
        "long_quote": long_quote,
    }

@asset(schedule=[selected_quotes])
def formatted_newsletter(context: dict) -> None:
    """
    Formats the newsletter.
    """
    from airflow.sdk import ObjectStoragePath

    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )

    date = context["dag_run"].run_after.strftime("%Y-%m-%d")
    selected_quotes_list = context["ti"].xcom_pull(
        dag_id="selected_quotes",
        task_ids=["selected_quotes"],
        key="return_value",
        include_prior_dates=True,
    )

    selected_quotes = selected_quotes_list[0]

    newsletter_template_path = (object_storage_path / "newsletter_template.txt")
    newsletter_template = newsletter_template_path.read_text()

    newsletter = newsletter_template.format(
        quote_text_1=selected_quotes["short_quote"]['q'],
        quote_author_1=selected_quotes["short_quote"]['a'],
        quote_text_2=selected_quotes["median_quote"]['q'],
        quote_author_2=selected_quotes["median_quote"]['a'],
        quote_text_3=selected_quotes["long_quote"]['q'],
        quote_author_3=selected_quotes["long_quote"]['a'],
        date=date,
    )

    date_newsletter_path = (object_storage_path / f"{date}_newsletter.txt")
    date_newsletter_path.write_text(newsletter)