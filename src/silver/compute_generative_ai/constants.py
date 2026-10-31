"""Constants for generative AI computations."""

from compute_generative_ai.enums import Maintainers

keyword_per_maintainer = {
    Maintainers.DEUTSCHE_WINDTECHNIK: r"availability|verfugbarkeiten",
    Maintainers.VESTAS: r"availability|disponibilidad",
    Maintainers.ENERCON: r"availability",
    Maintainers.NORDEX: r"availability|disponibilidad",
    Maintainers.SIEMENS: r"monthly\s+availability|availability\s+%|based\s+availability|availability\(%\)\s+for\s+the|disponibilidad\(%\)\s+de\s+los|plant\s+description|serial\s+number|disponibilidad",
    Maintainers.GAMESA: r"disponibilidad",
    Maintainers.GE: r"availability",
    Maintainers.UNKNOWN: r"availability",
    Maintainers.SUZLON: r"availability\s{2,}year|disponibilidad",
}

stop_keyword_per_maintainer = {
    Maintainers.NORDEX: r"memo",
}

# Pages always sent to Document Intelligence, even beyond PAGE_LIMIT / without
# the keyword (see page_selection.py). GE Willogoleche (Australia): contractual
# availability summary appended at the end + "WTG no. / Serial #" crosswalk.
priority_keyword_per_maintainer = {
    Maintainers.GE: r"contractual\s+availability\s+monthly\s+summary|serial\s*#",
}

shift_per_maintainer = {Maintainers.NORDEX: 15}

sheets_per_maintainer = {
    Maintainers.GE: [0, "Turbine Summary"],
    Maintainers.SUZLON: [0, "Availability Year"],
}

PAGE_LIMIT = 23

file_formats = {
    "excel": ["xlsx", "xls", "xlsm", "xlsb"],
    "pdf": ["pdf"],
}
