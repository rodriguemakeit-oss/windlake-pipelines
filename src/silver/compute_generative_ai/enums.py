"""Enums for generative AI computations."""

from enum import Enum


class TextExtraction(Enum):
    DOCUMENT_INTELLIGENCE = 1


class Maintainers(Enum):
    DEUTSCHE_WINDTECHNIK = "dwt"
    ENERCON = "enercon"
    GAMESA = "gamesa"
    GE = "ge"
    NORDEX = "nordex acciona"
    SIEMENS = "sgre"
    SUZLON = "suzlon"
    VESTAS = "vestas"
    UNKNOWN = "unknown"


class ExcelMode(Enum):
    PANDAS = "pandas"
    PANDAS_PLUS_DOCUMENT_INTELLIGENCE = "pandas_plus_di"
    OPENPYXL = "openpyxl"
