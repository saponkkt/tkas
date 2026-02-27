from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from thrust import get_config_param_series


def compute_eta_kg_per_min_per_kN(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.Series:
    """คำนวณคอลัมน์ eta_kg/min*kN ตามสูตร:

    eta = Cf1 * (1 + TAS_kt / Cf2)

    โดยที่ Cf1, Cf2 อ่านมาจาก `config.json` ตามชนิดเครื่องบิน (type_col).
    - หน่วย Cf1, Cf2 สมมติให้สอดคล้องกับสูตรที่กำหนด
    - คืนค่าเป็น Series ที่ index ตรงกับ df เดิม (ชื่อคอลัมน์: "eta_kg_per_min_per_kN")
    """
    try:
        cf1 = pd.to_numeric(get_config_param_series(df, "Cf1", type_col=type_col), errors="coerce")
        cf2 = pd.to_numeric(get_config_param_series(df, "Cf2", type_col=type_col), errors="coerce")
        tas = pd.to_numeric(df.get(tas_col), errors="coerce")

        eta = cf1 * (1.0 + (tas / cf2))

        eta.name = "eta_kg_per_min_per_kN"
        return eta
    except Exception:
        # return NaN series aligned with df
        s = pd.Series([pd.NA] * len(df), index=df.index, name="eta_kg_per_min_per_kN")
        return s


def add_eta_column(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `eta_kg_per_min_per_kN` เข้าไป.

    ใช้สูตรเดียวกับ `compute_eta_kg_per_min_per_kN`.
    """
    df_out = df.copy()
    df_out["eta_kg_per_min_per_kN"] = compute_eta_kg_per_min_per_kN(
        df_out, type_col=type_col, tas_col=tas_col
    )
    return df_out


def compute_fnom_kg_per_s(
    df: pd.DataFrame,
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
) -> pd.Series:
    """คำนวณคอลัมน์ fnom_kg/s ตามสูตร:

    fnom = eta_kg_per_min_per_kN * Thrust_N * 10^-3 / 60

    โดย:
    - `eta_kg_per_min_per_kN` มาจากคอลัมน์ eta ที่คำนวณจาก `compute_eta_kg_per_min_per_kN`
    - `Thrust_N` มาจากผลการคำนวณในไฟล์ thrust (คอลัมน์ชื่อเดียวกันใน DataFrame)
    """
    try:
        eta = pd.to_numeric(df.get(eta_col), errors="coerce")
        thrust = pd.to_numeric(df.get(thrust_col), errors="coerce")

        fnom = eta * thrust * 1e-3 / 60.0

        fnom.name = "fnom_kg_per_s"
        return fnom
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="fnom_kg_per_s")
        return s


def add_fnom_column(
    df: pd.DataFrame,
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fnom_kg_per_s`.

    - ถ้ายังไม่มีคอลัมน์ `eta_kg_per_min_per_kN` จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fnom_kg_per_s`.
    """
    df_out = df.copy()

    # ถ้ายังไม่มี eta ให้คำนวณเพิ่มก่อน
    if eta_col not in df_out.columns:
        df_out["eta_kg_per_min_per_kN"] = compute_eta_kg_per_min_per_kN(
            df_out, type_col=type_col, tas_col=tas_col
        )

    df_out["fnom_kg_per_s"] = compute_fnom_kg_per_s(
        df_out, thrust_col=thrust_col, eta_col=eta_col
    )
    return df_out


def compute_fmin_kg_per_s(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
) -> pd.Series:
    """คำนวณคอลัมน์ fmin_kg/s ตามสูตร:

    fmin = Cf3 * (1 - altitude / Cf4) / 60

    โดย:
    - `Cf3`, `Cf4` อ่านจาก `config.json` ตามชนิดเครื่องบิน (type_col)
    - `altitude` คือคอลัมน์ความสูง (หน่วย ft) ใน DataFrame
    """
    try:
        cf3 = pd.to_numeric(get_config_param_series(df, "Cf3", type_col=type_col), errors="coerce")
        cf4 = pd.to_numeric(get_config_param_series(df, "Cf4", type_col=type_col), errors="coerce")
        alt = pd.to_numeric(df.get(alt_col), errors="coerce")

        fmin = cf3 * (1.0 - (alt / cf4)) / 60.0

        fmin.name = "fmin_kg_per_s"
        return fmin
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="fmin_kg_per_s")
        return s


def add_fmin_column(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fmin_kg_per_s` เข้าไป.

    ใช้สูตรเดียวกับ `compute_fmin_kg_per_s`.
    """
    df_out = df.copy()
    df_out["fmin_kg_per_s"] = compute_fmin_kg_per_s(
        df_out, type_col=type_col, alt_col=alt_col
    )
    return df_out


def compute_fapld_kg_per_s(
    df: pd.DataFrame,
    fnom_col: str = "fnom_kg_per_s",
    fmin_col: str = "fmin_kg_per_s",
) -> pd.Series:
    """คำนวณคอลัมน์ fapld_kg/s โดยเลือกค่ามากสุดระหว่าง fnom และ fmin ในแต่ละบรรทัด.

    fapld_kg/s = max(fnom_kg_per_s, fmin_kg_per_s) ต่อแถว
    """
    try:
        fnom = pd.to_numeric(df.get(fnom_col), errors="coerce")
        fmin = pd.to_numeric(df.get(fmin_col), errors="coerce")

        fapld = pd.concat([fnom, fmin], axis=1).max(axis=1)

        fapld.name = "fapld_kg_per_s"
        return fapld
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="fapld_kg_per_s")
        return s


def add_fapld_column(
    df: pd.DataFrame,
    fnom_col: str = "fnom_kg_per_s",
    fmin_col: str = "fmin_kg_per_s",
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
    thrust_col: str = "Thrust_N",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fapld_kg_per_s`.

    - ถ้ายังไม่มี `fnom_kg_per_s` หรือ `fmin_kg_per_s` จะคำนวณเพิ่มให้อัตโนมัติ
    """
    df_out = df.copy()

    # ถ้ายังไม่มี fnom ให้คำนวณก่อน (ต้องมี eta และ Thrust_N)
    if fnom_col not in df_out.columns:
        df_out = add_fnom_column(
            df_out,
            thrust_col=thrust_col,
            eta_col="eta_kg_per_min_per_kN",
            type_col=type_col,
            tas_col=tas_col,
        )

    # ถ้ายังไม่มี fmin ให้คำนวณก่อน
    if fmin_col not in df_out.columns:
        df_out = add_fmin_column(
            df_out,
            type_col=type_col,
            alt_col=alt_col,
        )

    df_out["fapld_kg_per_s"] = compute_fapld_kg_per_s(
        df_out, fnom_col=fnom_col, fmin_col=fmin_col
    )

    return df_out


def compute_fcr_kg_per_s(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
) -> pd.Series:
    """คำนวณคอลัมน์ fcr_kg/s ตามสูตร:

    fcr = eta_kg_per_min_per_kN * Thrust_N * Cfcr * 10^-3 / 60

    โดย:
    - `eta_kg_per_min_per_kN` มาจากคอลัมน์ใน DataFrame
    - `Thrust_N` มาจากคอลัมน์ใน DataFrame
    - `Cfcr` อ่านจาก `config.json` ตามชนิดเครื่องบิน (type_col)
    """
    try:
        eta = pd.to_numeric(df.get(eta_col), errors="coerce")
        thrust = pd.to_numeric(df.get(thrust_col), errors="coerce")
        cfcr = pd.to_numeric(get_config_param_series(df, "Cfcr", type_col=type_col), errors="coerce")

        fcr = eta * thrust * cfcr * 1e-3 / 60.0

        fcr.name = "fcr_kg_per_s"
        return fcr
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="fcr_kg_per_s")
        return s


def add_fcr_column(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fcr_kg_per_s`.

    - ถ้ายังไม่มีคอลัมน์ `eta_kg_per_min_per_kN` จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fcr_kg_per_s`.
    """
    df_out = df.copy()

    # ถ้ายังไม่มี eta ให้คำนวณเพิ่มก่อน
    if eta_col not in df_out.columns:
        df_out["eta_kg_per_min_per_kN"] = compute_eta_kg_per_min_per_kN(
            df_out, type_col=type_col, tas_col=tas_col
        )

    df_out["fcr_kg_per_s"] = compute_fcr_kg_per_s(
        df_out, type_col=type_col, thrust_col=thrust_col, eta_col=eta_col
    )
    return df_out


def compute_fuel_kg_per_s(
    df: pd.DataFrame,
    phase_col: str = "flight_phase",
    fnom_col: str = "fnom_kg_per_s",
    fmin_col: str = "fmin_kg_per_s",
    fcr_col: str = "fcr_kg_per_s",
    fapld_col: str = "fapld_kg_per_s",
) -> pd.Series:
    """คำนวณคอลัมน์ Fuel_kg/s โดยเลือกค่าจากคอลัมน์อื่นๆ ตาม flight phase:

    1. Taxi_out, Descent, Taxi_in → ใช้ fmin_kg_per_s
    2. Takeoff, Initial_climb, Climb → ใช้ fnom_kg_per_s
    3. Cruise → ใช้ fcr_kg_per_s
    4. Approach, Landing → ใช้ fapld_kg_per_s
    """
    try:
        phases = df.get(phase_col)
        if phases is None:
            return pd.Series([pd.NA] * len(df), index=df.index, name="Fuel_kg_per_s")

        fnom = pd.to_numeric(df.get(fnom_col), errors="coerce")
        fmin = pd.to_numeric(df.get(fmin_col), errors="coerce")
        fcr = pd.to_numeric(df.get(fcr_col), errors="coerce")
        fapld = pd.to_numeric(df.get(fapld_col), errors="coerce")

        import numpy as _np
        fuel = pd.Series([_np.nan] * len(df), index=df.index, dtype=float)

        mask_taxi_descent = phases.isin(["Taxi_out", "Descent", "Taxi_in"])
        fuel.loc[mask_taxi_descent] = fmin.loc[mask_taxi_descent]

        mask_climb = phases.isin(["Takeoff", "Initial_climb", "Climb"])
        fuel.loc[mask_climb] = fnom.loc[mask_climb]

        mask_cruise = phases == "Cruise"
        fuel.loc[mask_cruise] = fcr.loc[mask_cruise]

        mask_approach_landing = phases.isin(["Approach", "Landing"])
        fuel.loc[mask_approach_landing] = fapld.loc[mask_approach_landing]

        fuel.name = "Fuel_kg_per_s"
        return fuel
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="Fuel_kg_per_s")
        return s


def add_fuel_column(
    df: pd.DataFrame,
    phase_col: str = "flight_phase",
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
    thrust_col: str = "Thrust_N",
    tas_col: str = "TAS_kt",
    fnom_col: str = "fnom_kg_per_s",
    fmin_col: str = "fmin_kg_per_s",
    fcr_col: str = "fcr_kg_per_s",
    fapld_col: str = "fapld_kg_per_s",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `Fuel_kg_per_s`.

    - ถ้ายังไม่มีคอลัมน์ที่ต้องใช้ (fnom, fmin, fcr, fapld) จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fuel_kg_per_s`.
    """
    df_out = df.copy()

    # ตรวจสอบและคำนวณคอลัมน์ที่จำเป็นตาม phase ที่มี
    phases = df_out.get(phase_col)
    if phases is None:
        # ถ้าไม่มี phase column ให้เพิ่มคอลัมน์ว่าง
        df_out["Fuel_kg_per_s"] = pd.Series([pd.NA] * len(df_out), index=df_out.index)
        return df_out

    # ตรวจสอบ phase ที่มีในข้อมูล
    unique_phases = set(phases.dropna().unique())

    # ถ้ามี phase ที่ต้องใช้ fnom
    if unique_phases & {"Takeoff", "Initial_climb", "Climb"}:
        if fnom_col not in df_out.columns:
            df_out = add_fnom_column(
                df_out,
                thrust_col=thrust_col,
                eta_col="eta_kg_per_min_per_kN",
                type_col=type_col,
                tas_col=tas_col,
            )

    # ถ้ามี phase ที่ต้องใช้ fmin
    if unique_phases & {"Taxi_out", "Descent", "Taxi_in"}:
        if fmin_col not in df_out.columns:
            df_out = add_fmin_column(
                df_out,
                type_col=type_col,
                alt_col=alt_col,
            )

    # ถ้ามี phase ที่ต้องใช้ fcr
    if "Cruise" in unique_phases:
        if fcr_col not in df_out.columns:
            df_out = add_fcr_column(
                df_out,
                type_col=type_col,
                thrust_col=thrust_col,
                eta_col="eta_kg_per_min_per_kN",
                tas_col=tas_col,
            )

    # ถ้ามี phase ที่ต้องใช้ fapld
    if unique_phases & {"Approach", "Landing"}:
        if fapld_col not in df_out.columns:
            df_out = add_fapld_column(
                df_out,
                fnom_col=fnom_col,
                fmin_col=fmin_col,
                type_col=type_col,
                alt_col=alt_col,
                thrust_col=thrust_col,
                tas_col=tas_col,
            )

    df_out["Fuel_kg_per_s"] = compute_fuel_kg_per_s(
        df_out,
        phase_col=phase_col,
        fnom_col=fnom_col,
        fmin_col=fmin_col,
        fcr_col=fcr_col,
        fapld_col=fapld_col,
    )

    return df_out


def compute_fuel_at_time_kg(
    df: pd.DataFrame,
    fuel_rate_col: str = "Fuel_kg_per_s",
    delta_t_col: str = "delta_t (s)",
) -> pd.Series:
    """คำนวณคอลัมน์ Fuel_at_time_kg ตามสูตร:

    Fuel_at_time_kg = Fuel_kg_per_s * delta_t (s)

    โดย:
    - `Fuel_kg_per_s` คืออัตราการใช้เชื้อเพลิงต่อวินาที
    - `delta_t (s)` คือช่วงเวลาระหว่างจุดข้อมูล (หน่วยวินาที) จากไฟล์ variable_mass.py
    """
    try:
        fuel_rate = pd.to_numeric(df.get(fuel_rate_col), errors="coerce")
        delta_t = pd.to_numeric(df.get(delta_t_col), errors="coerce")

        fuel_at_time = fuel_rate * delta_t

        fuel_at_time.name = "Fuel_at_time_kg"
        return fuel_at_time
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="Fuel_at_time_kg")
        return s


def add_fuel_at_time_column(
    df: pd.DataFrame,
    fuel_rate_col: str = "Fuel_kg_per_s",
    delta_t_col: str = "delta_t (s)",
    phase_col: str = "flight_phase",
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
    thrust_col: str = "Thrust_N",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `Fuel_at_time_kg`.

    - ถ้ายังไม่มีคอลัมน์ `Fuel_kg_per_s` จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fuel_at_time_kg`.
    - `delta_t_col` ใช้ชื่อคอลัมน์ `delta_t (s)` จากไฟล์ variable_mass.py เป็นค่า default
    """
    df_out = df.copy()

    # ถ้ายังไม่มี Fuel_kg_per_s ให้คำนวณก่อน
    if fuel_rate_col not in df_out.columns:
        df_out = add_fuel_column(
            df_out,
            phase_col=phase_col,
            type_col=type_col,
            alt_col=alt_col,
            thrust_col=thrust_col,
            tas_col=tas_col,
        )

    df_out["Fuel_at_time_kg"] = compute_fuel_at_time_kg(
        df_out, fuel_rate_col=fuel_rate_col, delta_t_col=delta_t_col
    )

    return df_out


def compute_fuel_sum_with_time_kg(
    df: pd.DataFrame,
    fuel_at_time_col: str = "Fuel_at_time_kg",
) -> pd.Series:
    """คำนวณคอลัมน์ Fuel_sum_with_time_kg โดยการบวกสะสมกันไปเรื่อยๆทีละบรรทัดของคอลัมน์ Fuel_at_time_kg.

    ตัวอย่าง:
    - บรรทัดแรก: Fuel_at_time_kg = 0 → Fuel_sum_with_time_kg = 0
    - บรรทัดที่ 2: Fuel_at_time_kg = 2 → Fuel_sum_with_time_kg = 0 + 2 = 2
    - บรรทัดที่ 3: Fuel_at_time_kg = 3 → Fuel_sum_with_time_kg = 2 + 3 = 5

    โดย:
    - `Fuel_at_time_kg` คือคอลัมน์ที่คำนวณจาก `compute_fuel_at_time_kg`
    """
    try:
        fuel_at_time = pd.to_numeric(df.get(fuel_at_time_col), errors="coerce")

        fuel_at_time_filled = fuel_at_time.fillna(0)
        fuel_sum = fuel_at_time_filled.cumsum()

        if len(fuel_sum) > 0 and pd.isna(fuel_at_time.iloc[0]):
            fuel_sum.iloc[0] = 0

        fuel_sum.name = "Fuel_sum_with_time_kg"
        return fuel_sum
    except Exception:
        s = pd.Series([pd.NA] * len(df), index=df.index, name="Fuel_sum_with_time_kg")
        return s


def add_fuel_sum_with_time_column(
    df: pd.DataFrame,
    fuel_at_time_col: str = "Fuel_at_time_kg",
    fuel_rate_col: str = "Fuel_kg_per_s",
    delta_t_col: str = "delta_t (s)",
    phase_col: str = "flight_phase",
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
    thrust_col: str = "Thrust_N",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `Fuel_sum_with_time_kg`.

    - ถ้ายังไม่มีคอลัมน์ `Fuel_at_time_kg` จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fuel_sum_with_time_kg`.
    """
    df_out = df.copy()

    # ถ้ายังไม่มี Fuel_at_time_kg ให้คำนวณก่อน
    if fuel_at_time_col not in df_out.columns:
        df_out = add_fuel_at_time_column(
            df_out,
            fuel_rate_col=fuel_rate_col,
            delta_t_col=delta_t_col,
            phase_col=phase_col,
            type_col=type_col,
            alt_col=alt_col,
            thrust_col=thrust_col,
            tas_col=tas_col,
        )

    df_out["Fuel_sum_with_time_kg"] = compute_fuel_sum_with_time_kg(
        df_out, fuel_at_time_col=fuel_at_time_col
    )

    return df_out
