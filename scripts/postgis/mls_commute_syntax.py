"""Construct the giant SQL statement that will make the commute cases."""
from wheretolive.postgis import PostGIS

# Could add more but let's start with these
_MODES = ["CAR", "WALK, TRANSIT", "WALK"]

MODES = [{"val": mode, "label": mode.replace(", ", "_")} for mode in _MODES]


PLACES = ["DOWNTOWN", "GF_WORK", "BROTHER", "TO_BC", "MOMMA_JILL"]

CUTOFF_TIMES = [i for i in range(10, 65, 5)]


def _make_commute_cte(mode, place):
    """Make something that can be used in a CTE for defining commutes."""
    sql = f"""{place}_{mode["label"]}_commutes AS (
        SELECT cutoff_time, geom
        FROM isochrones
        WHERE place_name = '{place}' AND commute_mode = '{mode["val"]}'
    )"""
    return sql


def make_all_commute_ctes():
    """Create CTEs for all the commute patterns."""
    ctes = list()
    for mode in MODES:
        for place in PLACES:
            ctes.append(_make_commute_cte(mode, place))
    cte_sql = ", \n".join(cte for cte in ctes)
    return f"WITH {cte_sql}"


def _make_commute_bool_case(mode_label, place, cutoff_time):
    commute_cte = f"{place}_{mode_label}_commutes"
    return f"""CASE
        WHEN ST_contains((SELECT geom FROM {commute_cte} WHERE cutoff_time = {cutoff_time}), geom)
        THEN True
        ELSE False
        END AS {place}_{mode_label}_{cutoff_time}
    """


def _make_commute_bools_case(mode_label, place):
    return ", \n".join(
        _make_commute_bool_case(mode_label, place, cutoff_time)
        for cutoff_time in CUTOFF_TIMES
    )


def _make_commute_label_case(mode_label, place):
    commute_cte = f"{place}_{mode_label}_commutes"
    whenthens = list()
    for cutoff_time in CUTOFF_TIMES:
        case = f"""WHEN ST_contains((SELECT geom FROM {commute_cte} WHERE cutoff_time = {cutoff_time}), geom)
        THEN 'up_to_{cutoff_time}_min'
        """
        whenthens.append(case)
    whenthens_str = "\n".join(whenthen for whenthen in whenthens)
    full_case = f"""CASE
    {whenthens_str}
    ELSE 'over_60_or_unknown'
    END AS {place}_{mode_label}_time
    """
    return full_case


def _make_commute_cases():
    cases = list()
    for mode in MODES:
        for place in PLACES:
            bool_cases = _make_commute_bools_case(mode["label"], place)
            label_case = _make_commute_label_case(mode["label"], place)
            all_cases = ", \n\n".join([bool_cases, label_case])
            cases.append(all_cases)
    return ", \n\n\n".join(case for case in cases)


def make_select():
    """Right now just a select, change to view creation later."""
    sql = f"""
    SELECT
    mls_id AS mls_commute_id,
    {_make_commute_cases()}
    FROM public.mls;
     """
    return sql


def make_sql():
    """Create the full SQL statement for commute logic."""
    return "\n\n\n".join(
        [
            "DROP VIEW IF EXISTS mls_commutes CASCADE;",
            "CREATE OR REPLACE VIEW mls_commutes AS",
            make_all_commute_ctes(),
            make_select(),
        ]
    )


sql = make_sql()
with PostGIS().connection.begin() as conn:
    result = conn.execute(sql)
print("All done")
