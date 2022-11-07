from .constants import Frequency, Period


def formatted_frequency(frequency: Frequency) -> str:
    if frequency is Frequency.ANNUALSEASONAL:
        return "Annual/Seasonal"
    else:
        return str(frequency.value).capitalize()


def id_string(frequency: Frequency, period: Period) -> str:
    return f"{period.value.replace('-', '_')}-{frequency}"
