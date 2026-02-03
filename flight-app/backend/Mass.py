from __future__ import annotations

import numpy as np
import pandas as pd


def add_P1_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P1` column computed.

    Formula:
    P1 = 2 * K * (9.80665**2) * (cos(gamma_rad)**2) /
         (Density * (TAS_m/s)**2 * S_m^2)

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities with NA.
    """
    df_out = df.copy()
    try:
        g = 9.80665
        K = pd.to_numeric(df_out.get("K"), errors="coerce")
        density = pd.to_numeric(df_out.get("Density"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")
        S = pd.to_numeric(df_out.get("S_m^2"), errors="coerce")
        gamma = pd.to_numeric(df_out.get("gamma_rad"), errors="coerce")

        cos2 = np.cos(gamma) ** 2
        denom = density * (tas ** 2) * S

        df_out["P1"] = (2 * K * (g ** 2) * cos2) / denom
        df_out["P1"] = df_out["P1"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P1"] = pd.NA

    return df_out

def add_P2_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P2` column computed.

    Formula:
    P2 = a_m/s^2 + 9.80665 * (ROCD_m/s) / (TAS_m/s)

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities or invalid values with NA.
    """
    df_out = df.copy()
    try:
        g = 9.80665
        a = pd.to_numeric(df_out.get("a_m/s^2"), errors="coerce")
        rocd = pd.to_numeric(df_out.get("ROCD_m/s"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")

        term = rocd / tas
        df_out["P2"] = a + (g * term)
        df_out["P2"] = df_out["P2"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P2"] = pd.NA

    return df_out

def add_P3_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P3` column computed.

    Formula:
    P3 = CD0 * 0.5 * Density * (TAS_m/s)^2 * S_m^2 - Thrust_N * 0.76

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities with NA.
    """
    df_out = df.copy()
    try:
        cd0 = pd.to_numeric(df_out.get("CD0"), errors="coerce")
        density = pd.to_numeric(df_out.get("Density"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")
        S = pd.to_numeric(df_out.get("S_m^2"), errors="coerce")
        thrust = pd.to_numeric(df_out.get("Thrust_N"), errors="coerce")

        aero_term = 0.5 * density * (tas ** 2) * S

        df_out["P3"] = (cd0 * aero_term) - (thrust * 0.76)
        df_out["P3"] = df_out["P3"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P3"] = pd.NA

    return df_out


def add_mt_column(
    df: pd.DataFrame,
    alt_col: str = "altitude",
    fuel_at_time_col: str = "Fuel_at_time_kg",
    mt_offset: float = 0.0,
) -> pd.DataFrame:
    
    """Return copy of `df` with new `mt` column computed per user spec.

    Rules implemented:
    1. Find the first row (earliest index) where `altitude` >= 10000. Set `mt` at
       that row to 0.
    2. For rows before that row (earlier in the dataframe), compute `mt` by
       accumulating backwards: mt[i] = mt[i+1] + Fuel_at_time_kg[i+1].
    3. For rows after that row (later in the dataframe), compute `mt` by
       subtracting forwards: mt[i] = mt[i-1] - Fuel_at_time_kg[i].

    The function will compute `Fuel_at_time_kg` first if it's missing by
    delegating to `Fuel.add_fuel_at_time_column` (imported locally to avoid
    circular imports). Inputs are converted to numeric defensively; infinities
    are treated as NA and fuel gaps filled with 0 for accumulation.
    """
    df_out = df.copy()
    try:
        # ensure Fuel_at_time_kg exists
        if fuel_at_time_col not in df_out.columns:
            # local import to avoid circular import at module level
            from Fuel import add_fuel_at_time_column

            df_out = add_fuel_at_time_column(df_out)

        alt = pd.to_numeric(df_out.get(alt_col), errors="coerce")
        fuel_at_time = pd.to_numeric(df_out.get(fuel_at_time_col), errors="coerce")

        # Treat NA fuel values as 0 for accumulation
        fuel_filled = fuel_at_time.fillna(0).astype(float)

        n = len(df_out)
        mt_values = [pd.NA] * n

        # find first position where altitude >= 10000
        mask = alt >= 10000
        true_pos = list(mask[mask].index)
        if len(true_pos) == 0:
            # no row reaches 10000 — cannot compute mt per spec
            df_out["mt"] = pd.Series([pd.NA] * n, index=df_out.index)
            return df_out

        # convert index to integer positions (first altitude >= 10000)
        pos0 = int(np.where(mask.values)[0][0])

        # set mt at crossing to 0.0
        mt_arr = np.full(n, np.nan, dtype=float)
        mt_arr[pos0] = 0.0

        # accumulate backwards: for i = pos0-1 .. 0: mt[i] = mt[i+1] + fuel_at_time[i+1]
        for i in range(pos0 - 1, -1, -1):
            mt_arr[i] = mt_arr[i + 1] + float(fuel_filled.iloc[i + 1])

        # accumulate forwards: for i = pos0+1 .. n-1: mt[i] = mt[i-1] - fuel_at_time[i]
        for i in range(pos0 + 1, n):
            mt_arr[i] = mt_arr[i - 1] - float(fuel_filled.iloc[i])

        # replace infinities / failed casts with NA
        mt_series = pd.Series(mt_arr, index=df_out.index, name="mt")
        mt_series = mt_series.replace([np.inf, -np.inf], pd.NA)

        # apply optional absolute offset (allows matching Excel absolute mass)
        try:
            mt_series = mt_series.astype(float) + float(mt_offset)
        except Exception:
            # if conversion fails, leave as-is
            pass

        df_out["mt"] = mt_series
    except Exception:
        df_out["mt"] = pd.NA

    return df_out


def compute_f2_series(
    df: pd.DataFrame,
    p1_col: str = "P1",
    p2_col: str = "P2",
    p3_col: str = "P3",
    mt_col: str = "mt",
) -> pd.Series:
    """Compute Series `f2 = P1*(mt)**2 + P2*mt + P3`.

    Defensive: converts inputs to numeric, treats infinities as NA.
    Returns a Series aligned with df index named `f2`.
    """
    try:
        p1 = pd.to_numeric(df.get(p1_col), errors="coerce")
        p2 = pd.to_numeric(df.get(p2_col), errors="coerce")
        p3 = pd.to_numeric(df.get(p3_col), errors="coerce")
        mt = pd.to_numeric(df.get(mt_col), errors="coerce")

        with pd.option_context("mode.use_inf_as_na", True):
            f2 = (p1 * (mt ** 2)) + (p2 * mt) + p3

        f2.name = "f2"
        f2 = f2.replace([np.inf, -np.inf], pd.NA)
        return f2
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="f2")
        return s


def add_f2_column(
    df: pd.DataFrame,
    p1_col: str = "P1",
    p2_col: str = "P2",
    p3_col: str = "P3",
    mt_col: str = "mt",
) -> pd.DataFrame:
    """Return DataFrame copy with `f2` column.

    If `mt` is missing, attempts to compute it using `add_mt_column`.
    """
    df_out = df.copy()
    try:
        if mt_col not in df_out.columns:
            # call local function to compute mt
            df_out = add_mt_column(df_out)

        df_out["f2"] = compute_f2_series(
            df_out, p1_col=p1_col, p2_col=p2_col, p3_col=p3_col, mt_col=mt_col
        )
    except Exception:
        df_out["f2"] = pd.NA

    return df_out


def compute_sumsq_series(
    df: pd.DataFrame,
    f2_col: str = "f2",
    alt_col: str = "altitude",
    alt_min: float = 10000.0,
    alt_max: float = 20000.0,
    phase_col: str = "flight_phase",
    phase_val: str = "Climb",
) -> pd.Series:
    """Compute Series `Sumsq` = sum(f2^2) for rows where alt in [alt_min, alt_max].

    The returned Series is aligned with `df` and filled with the scalar sumsq
    value for every row. If there are no valid f2 values in the altitude range
    the series contains `pd.NA`.
    """
    try:
        # ensure f2 exists (add if missing)
        if f2_col not in df.columns:
            df = add_f2_column(df)

        alt = pd.to_numeric(df.get(alt_col), errors="coerce")
        f2 = pd.to_numeric(df.get(f2_col), errors="coerce")

        # build phase mask (only include rows where flight_phase == phase_val)
        phases = df.get(phase_col)
        if phases is None:
            phase_mask = pd.Series([False] * len(df), index=df.index)
        else:
            phase_mask = phases == phase_val

        mask = (alt >= alt_min) & (alt <= alt_max) & phase_mask
        selected = f2[mask].dropna().astype(float)

        if len(selected) == 0:
            return pd.Series([pd.NA] * len(df), index=df.index, name="Sumsq")

        sumsq_val = (selected ** 2).sum()
        return pd.Series([sumsq_val] * len(df), index=df.index, name="Sumsq")
    except Exception:
        return pd.Series([pd.NA] * len(df), index=df.index, name="Sumsq")


def add_sumsq_column(
    df: pd.DataFrame,
    f2_col: str = "f2",
    alt_col: str = "altitude",
    alt_min: float = 10000.0,
    alt_max: float = 20000.0,
) -> pd.DataFrame:
    """Return DataFrame copy with `Sumsq` column added.

    If `f2` is missing it will be computed first.
    """
    df_out = df.copy()
    try:
        if f2_col not in df_out.columns:
            df_out = add_f2_column(df_out)

        df_out["Sumsq"] = compute_sumsq_series(
            df_out, f2_col=f2_col, alt_col=alt_col, alt_min=alt_min, alt_max=alt_max
        )
    except Exception:
        df_out["Sumsq"] = pd.NA

    return df_out


def optimize_mt0(
    df: pd.DataFrame,
    alt_col: str = "altitude",
    fuel_at_time_col: str = "Fuel_at_time_kg",
    f2_col: str = "f2",
    alt_min: float = 10000.0,
    alt_max: float = 20000.0,
    phase_col: str = "flight_phase",
    phase_val: str = "Climb",
    use_scipy: bool = True,
    mt_offset: float = 0.0,
    mt0_lower_bound=None,
    excel_nonneg: bool = False,
    target_mt0: float = None,
    weight_aero: float = 1.0,
    weight_target: float = 1.0,
):
    """Optimize mt at the first altitude>=10000 row to minimize combined objective.

    Returns (df_out, result) where df_out has updated `mt`, `f2`, `Sumsq` and
    `result` contains the optimized mt0 value and objective value.

    Strategy:
    - Find first position pos0 where altitude >= 10000.
    - Treat mt[pos0] as the single optimization variable `mt0`.
    - Given mt0, rebuild full `mt` array by accumulating fuel (backwards and
      forwards) using the same rules as `add_mt_column` but with mt[pos0]=mt0.
    - Compute `f2` and `Sumsq` (restricted to phase==phase_val and alt range).
    - Minimize combined objective: weight_aero*Sumsq + weight_target*(mt0-target_mt0)^2
      over mt0 using `scipy.optimize.minimize_scalar` if available and requested;
      otherwise use a grid + refinement search.
    
    Parameters:
    - target_mt0: if provided, optimizer will try to match mt[0] close to this value
    - weight_aero: relative weight for aerodynamic (Sumsq) term (default 1.0)
    - weight_target: relative weight for target matching term (default 1.0)
    """
    df_in = df.copy()
    # ensure fuel_at_time exists
    if fuel_at_time_col not in df_in.columns:
        from Fuel import add_fuel_at_time_column

        df_in = add_fuel_at_time_column(df_in)

    # ensure P1,P2,P3 exist
    if "P1" not in df_in.columns:
        df_in = add_P1_column(df_in)
    if "P2" not in df_in.columns:
        df_in = add_P2_column(df_in)
    if "P3" not in df_in.columns:
        df_in = add_P3_column(df_in)

    alt = pd.to_numeric(df_in.get(alt_col), errors="coerce")
    fuel_at_time = pd.to_numeric(df_in.get(fuel_at_time_col), errors="coerce").fillna(0).astype(float)

    mask_cross = alt >= 10000.0
    if not mask_cross.any():
        # per spec: only optimize when a row altitude >= 10000 exists
        # return mt computed by add_mt_column and indicate skipped optimization
        df_out = add_mt_column(df_in)
        result = {"mt0": None, "objective": None, "pos0": None, "skipped": "no_alt_ge_10000"}
        return df_out, result

    pos0 = int(np.where(mask_cross.values)[0][0])

    # require that the anchor row is in the requested phase (e.g. 'Climb')
    phases = df_in.get(phase_col)
    if phases is None or phases.iloc[pos0] != phase_val:
        df_out = add_mt_column(df_in)
        result = {"mt0": None, "objective": None, "pos0": pos0, "skipped": "pos0_not_in_phase"}
        return df_out, result
    n = len(df_in)

    # helpers to build mt from mt0 (initial mass offset)
    # Use cumulative sum: mt[i] = mt0 - cumsum_fuel[i]
    def build_mt_from_mt0(mt0: float) -> np.ndarray:
        try:
            fuel_sum = fuel_at_time.cumsum().astype(float)
            mt_arr = (float(mt0) - fuel_sum).to_numpy(dtype=float)
            return mt_arr
        except Exception:
            # fallback: return array of NaN
            return np.full(n, np.nan, dtype=float)

    # objective: compute Sumsq scalar for given mt0
    p1 = pd.to_numeric(df_in.get("P1"), errors="coerce").astype(float)
    p2 = pd.to_numeric(df_in.get("P2"), errors="coerce").astype(float)
    p3 = pd.to_numeric(df_in.get("P3"), errors="coerce").astype(float)
    phases = df_in.get(phase_col)

    def objective(mt0: float) -> float:
        mt_arr = build_mt_from_mt0(mt0)
        # compute f2 per row
        f2_arr = (p1.values * (mt_arr ** 2)) + (p2.values * mt_arr) + p3.values
        # mask by altitude and phase
        phase_mask = (phases == phase_val) if phases is not None else pd.Series([False] * n, index=df_in.index)
        sel_mask = (alt >= alt_min) & (alt <= alt_max) & phase_mask
        selected = f2_arr[sel_mask.values]
        # drop NaNs
        selected = selected[~np.isnan(selected)]
        if selected.size == 0:
            # no valid rows in the required altitude/phase window — skip optimization
            sumsq_term = float("inf")
        else:
            sumsq_term = float(np.sum(selected ** 2))
        
        # Combine aerodynamic objective with target matching if provided
        if target_mt0 is not None and weight_target > 0:
            # Include a term to minimize distance from target mt[0] value
            target_error = float(mt0 - target_mt0) ** 2
            # Combined objective: balance aerodynamic fit with target matching
            combined_obj = weight_aero * sumsq_term + weight_target * target_error
            return combined_obj
        else:
            # Only minimize Sumsq
            return sumsq_term

    # bounds for mt0: heuristic based on P1/P3 magnitudes, fallback to a large default
    p1_arr = pd.to_numeric(df_in.get("P1"), errors="coerce").astype(float).values
    p3_arr = pd.to_numeric(df_in.get("P3"), errors="coerce").astype(float).values
    # estimate scale: sqrt(|P3| / min_positive_P1)
    try:
        pos_p1 = p1_arr[p1_arr > 0]
        max_abs_p3 = np.nanmax(np.abs(p3_arr)) if p3_arr.size > 0 else 0.0
        if pos_p1.size > 0 and max_abs_p3 > 0:
            min_pos_p1 = float(np.min(pos_p1))
            if min_pos_p1 > 1e-10:  # Avoid division by very small numbers
                est = float(np.sqrt(max_abs_p3 / min_pos_p1))
                # tighter, more conservative bound: scale estimate by 2
                # and ensure a reasonable minimum bound to allow typical aircraft masses
                # Cap bound at 1e6 to avoid overflow
                bound = max(1e5, min(1e6, est * 2.0))
            else:
                bound = 1e5
        else:
            total_fuel = float(np.abs(fuel_at_time).sum())
            # fallback: use total fuel scaled but keep lower baseline
            bound = max(1e5, total_fuel * 100.0)
    except Exception:
        bound = 1e6

    # determine search bounds. By default allow negative/positive search
    # unless caller requests Excel-like non-negative behavior or supplies
    # an explicit lower bound.
    if excel_nonneg:
        # When requiring non-negative mt values, set lo to ensure mt stays non-negative
        # even at the end of flight: mt[end] = mt0 - total_fuel >= 0
        # So: mt0 >= total_fuel
        total_fuel_burned = float(np.abs(fuel_at_time).sum())
        lo = max(0.0, total_fuel_burned)
        # If target_mt0 is provided, ensure the upper bound allows it
        if target_mt0 is not None:
            hi = max(bound, float(target_mt0) * 1.1)  # Allow 10% margin above target
        else:
            hi = bound
    elif mt0_lower_bound is not None:
        try:
            lo = float(mt0_lower_bound)
        except Exception:
            lo = -bound
        hi = bound
    else:
        lo, hi = -bound, bound

    # try scipy first if requested
    mt0_opt = None
    obj_opt = None
    if use_scipy:
        try:
            from scipy.optimize import minimize_scalar

            res = minimize_scalar(objective, bounds=(lo, hi), method="bounded", options={"xatol": 1e-6})
            if res.success:
                mt0_opt = float(res.x)
                obj_opt = float(res.fun)
        except Exception:
            mt0_opt = None

    # fallback grid + refinement if scipy missing or failed
    if mt0_opt is None:
        # coarse grid
        best_x = None
        best_y = float("inf")
        for x in np.linspace(lo, hi, 401):
            y = objective(float(x))
            if y < best_y:
                best_y = y
                best_x = float(x)
        # refine around best_x
        span = (hi - lo) / 20.0
        for _ in range(4):
            lo_r = best_x - span
            hi_r = best_x + span
            xs = np.linspace(lo_r, hi_r, 401)
            for x in xs:
                y = objective(float(x))
                if y < best_y:
                    best_y = y
                    best_x = float(x)
            span /= 10.0

        mt0_opt = best_x
        obj_opt = best_y

    # build final df_out with optimized mt, f2, Sumsq
    mt_final = build_mt_from_mt0(mt0_opt)
    # apply optional absolute offset so callers can request absolute mt
    try:
        mt_final = mt_final.astype(float) + float(mt_offset)
    except Exception:
        try:
            mt_final = mt_final + float(mt_offset)
        except Exception:
            pass
    df_out = df_in.copy()
    df_out["mt"] = pd.Series(mt_final, index=df_out.index)
    df_out["f2"] = (pd.to_numeric(df_out.get("P1"), errors="coerce") * (df_out["mt"] ** 2)) + (
        pd.to_numeric(df_out.get("P2"), errors="coerce") * df_out["mt"]
    ) + pd.to_numeric(df_out.get("P3"), errors="coerce")
    df_out["f2"] = df_out["f2"].replace([np.inf, -np.inf], pd.NA)
    df_out["Sumsq"] = compute_sumsq_series(
        df_out, f2_col=f2_col, alt_col=alt_col, alt_min=alt_min, alt_max=alt_max, phase_col=phase_col, phase_val=phase_val
    )

    result = {"mt0": mt0_opt, "objective": obj_opt, "pos0": pos0}
    return df_out, result




