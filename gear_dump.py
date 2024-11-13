#!/usr/bin/env python3
import polars as pl

from stravalib.client import Client
from strava_client import get_authorized_client

from tabulate import tabulate


def get_gear(client: Client) -> dict[str | None, str | None]:
    return {b.id: b.name for b in client.get_athlete().bikes or []}


def get_all_activities(client: Client, gear) -> pl.DataFrame:
    """Fetch all activities with basic stats by gear."""
    activities = [
        {
            "gear_id": activity.gear_id,
            "gear_name": gear.get(activity.gear_id, "unknown"),
            "Date": activity.start_date_local.strftime("%Y-%m-%d")
            if activity.start_date_local
            else "unknown",
            "Distance (km)": f"{float(activity.distance) / 1000}"
            if activity.distance
            else "unknown",
            "Time": f"{activity.moving_time.timedelta().seconds // 3600 + activity.moving_time.timedelta().days * 24:02}:{(activity.moving_time.timedelta().seconds // 60) % 60:02}"
            if activity.moving_time
            else "unknown",
        }
        for activity in client.get_activities()
        if activity.gear_id
    ]

    return pl.DataFrame(activities)


def print_gear_tables(df: pl.DataFrame) -> None:
    """Print basic stats tables for each piece of gear."""
    if df.is_empty():
        print("No activities found.")
        return

    display_columns = ["Date", "Distance (km)", "Time"]

    for gear_id in df["gear_id"].unique():
        gear_df = df.filter(pl.col("gear_id") == gear_id)
        gear_name = gear_df["gear_name"][0]

        print(f"\n=== {gear_name} ({gear_id}) ===")

        # Sort by date and select columns
        gear_table = (
            gear_df.select(display_columns).sort("Date", descending=True).to_dicts()
        )

        print(
            tabulate(
                gear_table,
                headers="keys",
                tablefmt="grid",
                showindex=False,
                floatfmt=".1f",
            )
        )


if __name__ == "__main__":
    client = get_authorized_client()
    gear = get_gear(client)
    activities_df = get_all_activities(client, gear)
    print_gear_tables(activities_df)
