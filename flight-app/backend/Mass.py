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

        df_out["P3"] = (cd0 * aero_term) - (thrust * 0.74)
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

        # find first position where altitude >= 15000
        mask = alt >= 15000
        true_pos = list(mask[mask].index)
        if len(true_pos) == 0:
            # no row reaches 15000 — cannot compute mt per spec
            df_out["mt"] = pd.Series([pd.NA] * n, index=df_out.index)
            return df_out

        # convert index to integer positions (first altitude >= 15000)
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
    alt_min: float = 15000.0,
    alt_max: float = 25000.0,
    phase_col: str = "flight_phase",
    phase_val: str = "Climb",
    a_col: str = "a_m/s^2",
    a_min: float = -3.0,
    a_max: float = 3.0,
) -> pd.Series:
    """Compute Series `Sumsq` = sum(f2^2) for rows where:
    - alt in [alt_min, alt_max]
    - flight_phase == phase_val
    - a_m/s^2 IN [a_min, a_max]

    The returned Series is aligned with `df` and filled with the scalar sumsq
    value for every row. If there are no valid f2 values meeting all conditions,
    the series contains `pd.NA`.
    """
    try:
        # ensure f2 exists (add if missing)
        if f2_col not in df.columns:
            df = add_f2_column(df)

        alt = pd.to_numeric(df.get(alt_col), errors="coerce")
        f2 = pd.to_numeric(df.get(f2_col), errors="coerce")
        a = pd.to_numeric(df.get(a_col), errors="coerce")
        p2 = pd.to_numeric(df.get(p2_col), errors="coerce")

        # build phase mask (only include rows where flight_phase == phase_val)
        phases = df.get(phase_col)
        if phases is None:
            phase_mask = pd.Series([False] * len(df), index=df.index)
        else:
            phase_mask = phases == phase_val

        # build acceleration mask (only include rows where a_m/s^2 IN [a_min, a_max])
        a_mask = (a >= a_min) & (a <= a_max)

        mask = (alt >= alt_min) & (alt <= alt_max) & phase_mask & a_mask
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
    alt_min: float = 15000.0,
    alt_max: float = 25000.0,
    a_col: str = "a_m/s^2",
    a_min: float = -3.0,
    a_max: float = 3.0,
) -> pd.DataFrame:
    """Return DataFrame copy with `Sumsq` column added.

    If `f2` is missing it will be computed first.
    Filters by altitude range, flight phase, and acceleration range.
    """
    df_out = df.copy()
    try:
        if f2_col not in df_out.columns:
            df_out = add_f2_column(df_out)

        df_out["Sumsq"] = compute_sumsq_series(
            df_out, 
            f2_col=f2_col, 
            alt_col=alt_col, 
            alt_min=alt_min, 
            alt_max=alt_max,
            a_col=a_col,
            a_min=a_min,
            a_max=a_max,
        )
    except Exception:
        df_out["Sumsq"] = pd.NA

    return df_out


def optimize_mt0(
    df: pd.DataFrame,
    alt_col: str = "altitude",
    fuel_at_time_col: str = "Fuel_at_time_kg",
    f2_col: str = "f2",
    alt_min: float = 15000.0,
    alt_max: float = 25000.0,
    phase_col: str = "flight_phase",
    phase_val: str = "Climb",
    a_col: str = "a_m/s^2",
    a_min: float = -3.0,
    a_max: float = 3.0,
    use_scipy: bool = True,
    mt_offset: float = 0.0,
    mt0_lower_bound=None,
    excel_nonneg: bool = False,
    target_mt0: float = None,
    weight_aero: float = 1.0,
    weight_target: float = 1.0,
):
    """Optimize mt[0] (ETOW) to minimize Sumsq aerodynamic residuals.

    Returns (df_out, result) where df_out has updated `mt`, `f2`, `Sumsq` and
    `result` contains the optimized mt0 value and objective value.

    Strategy:
    - Treat mt[0] (ETOW) as the single optimization variable.
    - Given mt[0], rebuild full `mt` array: mt[i] = mt[0] - cumsum(Fuel_at_time[i])
    - Compute `f2 = P1*(mt)^2 + P2*mt + P3` for all rows
    - Compute `Sumsq = sum(f2^2)` restricted to:
      * phase == phase_val
      * alt_min <= altitude <= alt_max
      * a_min <= a_m/s^2 <= a_max
    - Minimize combined objective: weight_aero*Sumsq + weight_target*(mt[0]-target_mt[0])^2
      using scipy.optimize.minimize_scalar if available; otherwise grid search.
    
    Parameters:
    - target_mt0: if provided, optimizer includes penalty term to match mt[0] to this value
    - weight_aero: relative weight for aerodynamic (Sumsq) term (default 1.0)
    - weight_target: relative weight for target matching term (default 1.0)
    - a_col: column name for acceleration (default "a_m/s^2")
    - a_min, a_max: acceleration bounds (default -3.0 to 3.0)
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
    a = pd.to_numeric(df_in.get(a_col), errors="coerce")

    # Verify we can optimize: need at least one row where all conditions match
    mask_alt = (alt >= alt_min) & (alt <= alt_max)
    mask_a = (a >= a_min) & (a <= a_max)
    phases = df_in.get(phase_col)
    if phases is not None:
        mask_phase = (phases == phase_val)
    else:
        mask_phase = pd.Series([False] * len(df_in), index=df_in.index)
    
    mask_valid = mask_alt & mask_a & mask_phase
    
    if not mask_valid.any():
        # Cannot optimize: no valid rows to minimize over
        df_out = add_mt_column(df_in)
        result = {"mt0": None, "objective": None, "skipped": "no_valid_rows"}
        return df_out, result

    n = len(df_in)

    # Build mt array from mt[0] value: mt[i] = mt[0] - cumsum_fuel[i]
    # Reference point: set mt = mt0 at first altitude >= alt_min
    def build_mt_from_mt0(mt0: float) -> np.ndarray:
        try:
            # หา position แรกที่ alt >= alt_min
            mask = alt >= alt_min
            true_pos = list(mask[mask].index)
            
            if len(true_pos) == 0:
                # ถ้าไม่มี altitude >= alt_min ใช้วิธีเดิม
                fuel_sum = fuel_at_time.cumsum().astype(float)
                mt_arr = (float(mt0) - fuel_sum).to_numpy(dtype=float)
                return mt_arr
            
            # หา integer position ของ crossing
            pos0 = int(np.where(mask.values)[0][0])
            
            # mt[pos0] = mt0 (optimize value at reference point)
            fuel_arr = fuel_at_time.values.astype(float)
            mt_arr = np.full(n, np.nan, dtype=float)
            mt_arr[pos0] = float(mt0)
            
            # Forward: i > pos0: mt[i] = mt[i-1] - fuel[i]
            for i in range(pos0 + 1, n):
                mt_arr[i] = mt_arr[i - 1] - float(fuel_arr[i])
            
            # Backward: i < pos0: mt[i] = mt[i+1] + fuel[i+1]
            for i in range(pos0 - 1, -1, -1):
                mt_arr[i] = mt_arr[i + 1] + float(fuel_arr[i + 1])
            
            return mt_arr
        except Exception:
            return np.full(n, np.nan, dtype=float)

    # Objective function: compute Sumsq for given mt[0]
    p1 = pd.to_numeric(df_in.get("P1"), errors="coerce").astype(float)
    p2 = pd.to_numeric(df_in.get("P2"), errors="coerce").astype(float)
    p3 = pd.to_numeric(df_in.get("P3"), errors="coerce").astype(float)

    def objective(mt0: float) -> float:
        mt_arr = build_mt_from_mt0(mt0)
        # compute f2 per row: f2 = P1*mt^2 + P2*mt + P3
        f2_arr = (p1.values * (mt_arr ** 2)) + (p2.values * mt_arr) + p3.values
        
        # Apply SAME mask as Sumsq: altitude + phase + acceleration
        sel_mask = mask_alt & mask_phase & mask_a
        selected = f2_arr[sel_mask.values]
        # drop NaNs
        selected = selected[~np.isnan(selected)]
        
        if selected.size == 0:
            sumsq_term = float("inf")
        else:
            sumsq_term = float(np.sum(selected ** 2))
        
        # Combine aerodynamic objective with optional target matching
        if target_mt0 is not None and weight_target > 0:
            target_error = float(mt0 - target_mt0) ** 2
            combined_obj = weight_aero * sumsq_term + weight_target * target_error
            return combined_obj
        else:
            return sumsq_term

    # Determine search bounds
    total_fuel_burned = float(np.abs(fuel_at_time).sum())
    
    if excel_nonneg:
        # Ensure mt stays non-negative throughout flight
        # mt[end] = mt[0] - total_fuel >= 0 => mt[0] >= total_fuel
        lo = max(total_fuel_burned * 1.05, 10000.0)
        if target_mt0 is not None:
            hi = max(total_fuel_burned * 1.1, float(target_mt0) * 1.2)
        else:
            hi = total_fuel_burned + 100000.0
    elif mt0_lower_bound is not None:
        try:
            lo = float(mt0_lower_bound)
        except Exception:
            lo = max(total_fuel_burned, 0.0)
        hi = lo + 200000.0
    else:
        # Default: reasonable range for typical aircraft
        lo = max(total_fuel_burned, 10000.0)
        hi = lo + 150000.0

    # Optimize using scipy if available
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

    # Fallback: grid search + refinement
    if mt0_opt is None:
        best_x = None
        best_y = float("inf")
        # Coarse grid
        for x in np.linspace(lo, hi, 401):
            y = objective(float(x))
            if y < best_y:
                best_y = y
                best_x = float(x)
        
        # Refine around best point
        if best_x is not None:
            span = (hi - lo) / 20.0
            for _ in range(4):
                lo_r = best_x - span
                hi_r = best_x + span
                for x in np.linspace(lo_r, hi_r, 401):
                    y = objective(float(x))
                    if y < best_y:
                        best_y = y
                        best_x = float(x)
                span /= 10.0
        
        mt0_opt = best_x if best_x is not None else lo
        obj_opt = best_y

    # Build final output with optimized mt
    mt_final = build_mt_from_mt0(mt0_opt)
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
    # Compute Sumsq with SAME conditions
    df_out["Sumsq"] = compute_sumsq_series(
        df_out, 
        f2_col=f2_col, 
        alt_col=alt_col, 
        alt_min=alt_min, 
        alt_max=alt_max, 
        phase_col=phase_col, 
        phase_val=phase_val,
        a_col=a_col,
        a_min=a_min,
        a_max=a_max,
    )

    result = {"mt0": mt0_opt, "etow": mt0_opt, "objective": obj_opt}
    return df_out, result
