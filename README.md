# Change Log

## [0.0.2] - 2026-05-13

### Added

- `RiskAssessment/FY_2025/LOBs/CPB/Hua/CPB_301368_Contact-Center_Centralized_FY2025.ipynb` — Contact Center centralized metrics notebook (AU 301368). Covers BDE 1.1–1.5, 1.7/1.8, SD2, SD6, 3.17, 3.18.
- `RiskAssessment/FY_2025/LOBs/CPB/Hua/CPB_301365_ATM-Channel_Centralized_FY2025.ipynb` — ATM Channel centralized metrics notebook (AU 301365). Same metrics as Contact Center plus BDE 3.19 (LCTRs).

### Changed

- Renamed original Contact Center notebook to `CPB_301368_Contact-Center_Centralized_FY2025_debugged.ipynb` (archived with debug/reconciliation cells).

### Notes

- Both notebooks share identical customer base logic (`SELECT DISTINCT acc.customr_num, acc.customr_type`) and unified variable naming for easy cross-notebook reuse.
- Notebook headers follow Section 8.1 mandatory format with JIRA ticket references.
- Rated customer table loaded once and reused across metrics 1.2–1.5.

## [0.0.1] - 2026-05-05

### Added

- Initial commit with Databricks examples.