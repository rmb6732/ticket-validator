# Ticket Validation for RSC

This project processes and validates daily tickets against a master list of tickets from the NMS system.  
It uses **Polars** for efficient data wrangling and joins.

---

## 📌 Features
- Extracts **site codes** from `short_description` in the daily tickets file.
- Aggregates the **latest alarm text and time** from the tickets file (per `Controlling Object Name`).
- Joins daily tickets with historical tickets based on `site_code`.
- Adds validation logic:
  - **VALID** if the alarm in `ALARMS` matches the one in the NMS.
  - **INVALID** if it exists in NMS but does not match.
  - **NOT IN NMS** if the site code cannot be found in NMS.
- Supports large datasets using **lazy Polars queries**.

---

## NOTES
- Ensure that both `daily_tickets.csv` and `tickets.csv` have consistent column names.
- `short_description` and `ALARMS` from `daily_tickets`
- `Controlling Object Name`, `Origin Alarm Time`, `Alarm Text` in tickets from `NMS`
- **DO NOT** modify the exports of `daily_tickets.csv` and `tickets.csv`.
