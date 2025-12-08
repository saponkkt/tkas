import math


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on the Earth in nautical miles.

    Parameters
    ----------
    lat1, lon1, lat2, lon2 : float
        Latitude and longitude of two points in decimal degrees.
    """
    # Earth radius in nautical miles
    R_NM = 3440.065

    # Convert decimal degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2
    ) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R_NM * c


