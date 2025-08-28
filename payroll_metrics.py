#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import oci
from oci.monitoring import MonitoringClient
from oci.monitoring.models import SummarizeMetricsDataDetails
import argparse
import csv
import sys
import json
import re
import os
from datetime import datetime, timedelta, timezone

# -----------------------
# Utilidades auxiliares
# -----------------------

def iso8601_to_datetime(ts: str) -> datetime:
    """Convierte un RFC3339 '...Z' a datetime UTC."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def parse_timeframe(tf_str: str):
    """
    Acepta:
      1) Absoluto: [MM-DD-YYYY:HH:MM:SS]-[MM-DD-YYYY:HH:MM:SS] (se interpreta como UTC)
      2) Relativo: <int><unit> con unidades s,m,h,d,w,mo (mo ≈ 30 días)
    Devuelve (start_dt_utc, end_dt_utc) en datetime timezone.utc y una etiqueta timeframe_label segura para filename.
    """
    now_utc = datetime.now(timezone.utc)

    # 1) absoluto
    m = re.match(
        r'^\[(\d{2}-\d{2}-\d{4}:\d{2}:\d{2}:\d{2})\]-\[(\d{2}-\d{2}-\d{4}:\d{2}:\d{2}:\d{2})\]$',
        tf_str.strip()
    )
    if m:
        fmt = "%m-%d-%Y:%H:%M:%S"
        start = datetime.strptime(m.group(1), fmt).replace(tzinfo=timezone.utc)
        end = datetime.strptime(m.group(2), fmt).replace(tzinfo=timezone.utc)
        if end <= start:
            raise ValueError("El end_time debe ser posterior a start_time.")
        # etiqueta compacta para filename
        label = f"{start.strftime('%m%d%Y_%H%M%S')}-{end.strftime('%m%d%Y_%H%M%S')}"
        return start, end, label

    # 2) relativo
    m = re.match(r'^(\d+)\s*(s|m|h|d|w|mo)$', tf_str.strip(), re.IGNORECASE)
    if m:
        qty = int(m.group(1))
        unit = m.group(2).lower()
        if unit == 's':
            delta = timedelta(seconds=qty)
        elif unit == 'm':
            delta = timedelta(minutes=qty)
        elif unit == 'h':
            delta = timedelta(hours=qty)
        elif unit == 'd':
            delta = timedelta(days=qty)
        elif unit == 'w':
            delta = timedelta(weeks=qty)
        elif unit == 'mo':
            delta = timedelta(days=30 * qty)  # aproximación
        else:
            raise ValueError("Unidad no soportada.")
        start = now_utc - delta
        end = now_utc
        label = f"last{qty}{unit}"
        return start, end, label

    raise ValueError("Formato de timeframe inválido.")

def sanitize_query(tokens) -> str:
    """
    Reensambla tokens y quita comillas exteriores si vienen como '...'
    o "..." (muy común en PowerShell/PyCharm).
    """
    q = " ".join(tokens).strip()
    if len(q) >= 2 and q[0] == q[-1] and q[0] in ("'", '"'):
        q = q[1:-1]
    return q

def extract_lb_name_from_query(mql: str) -> str:
    """
    Intenta extraer lbName="..." o resourceName="..." de la MQL.
    Devuelve 'unknownlb' si no encuentra nada.
    """
    for key in ("lbName", "resourceName"):
        m = re.search(rf'{key}\s*=\s*"([^"]+)"', mql)
        if m:
            return m.group(1)
    return "unknownlb"

def sanitize_for_filename(s: str) -> str:
    """
    Sanea texto para usarlo en filename: quita o reemplaza caracteres problemáticos.
    """
    s = s.strip().replace(" ", "_")
    # elimina cualquier cosa que no sea alfanum, guion, guion_bajo o punto
    s = re.sub(r'[^A-Za-z0-9._-]+', '-', s)
    s = re.sub(r'-+', '-', s).strip('-')
    return s or "unnamed"

def build_output_path(out_dir: str, lb_name: str, region: str, timeframe_label: str) -> str:
    fname = f"metrics_{sanitize_for_filename(lb_name)}_{sanitize_for_filename(region)}_{sanitize_for_filename(timeframe_label)}.json"
    return os.path.join(out_dir or ".", fname)

# -----------------------
# Resolución de compartment por nombre u OCID (opcional)
# -----------------------

OCID_RX = re.compile(r"^ocid1\.compartment\..+$")

def resolve_compartment_id(identity_client, tenancy_id: str, arg: str) -> str:
    # Si parece OCID, úsalo tal cual
    if OCID_RX.match(arg):
        return arg

    # Si es nombre, buscar en todo el subárbol accesible
    tenancy = identity_client.get_tenancy(tenancy_id).data
    if tenancy.name == arg:
        return tenancy_id

    compartments = oci.pagination.list_call_get_all_results(
        identity_client.list_compartments,
        tenancy_id,
        access_level="ACCESSIBLE",
        compartment_id_in_subtree=True
    ).data

    for c in compartments:
        if c.name == arg:
            return c.id

    raise ValueError(
        f"No se encontró un compartment con nombre '{arg}'. "
        f"Provee el OCID o verifica el nombre exacto."
    )

# -----------------------
# CLI
# -----------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Consulta métricas de OCI y devuelve CSV + JSON",
        fromfile_prefix_chars='@'
    )
    parser.add_argument("--config-file", default="auth/config", help="Ruta al archivo de configuración de OCI (default: auth/config)")
    parser.add_argument("--profile", default="DEFAULT", help="Nombre del perfil en el config de OCI (default: DEFAULT)")
    parser.add_argument("--compartment-id", required=True, help="OCID o nombre del compartimento")
    parser.add_argument("--namespace", default="oci_lbaas", help="Namespace de métricas (default: oci_lbaas)")
    parser.add_argument("--query", nargs="+", default=['HttpRequests[1m]{lbName = "payroll-lbaas-flex_payroll_prod"}.sum()'], help="MQL. Ej: 'HttpRequests[1m]{lbName = \"...\"}.sum()'")
    parser.add_argument("--timeframe", help="Rango: [MM-DD-YYYY:HH:MM:SS]-[MM-DD-YYYY:HH:MM:SS] o relativo 15m/6h/2d/1w/3mo")
    parser.add_argument("--start-time", help="Inicio RFC3339 (ej: 2025-08-21T20:00:00Z)")
    parser.add_argument("--end-time", help="Fin RFC3339 (ej: 2025-08-21T21:00:00Z)")
    parser.add_argument("--resolution", help="Resolución opcional (ej: 1m, 5m, 1h)")
    parser.add_argument("--resource-group", help="Resource group opcional")
    parser.add_argument("--out-dir", default=".", help="Directorio de salida para el JSON (default: .)")
    parser.add_argument("--debug", action="store_true", help="Imprime detalles y activa logging del SDK")
    return parser.parse_args()

# -----------------------
# Main
# -----------------------

def main():
    args = parse_args()

    # Logging detallado si se pide
    if args.debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        oci.base_client.is_debug = True

    # Autenticación
    config = oci.config.from_file(args.config_file, args.profile)
    region = config.get("region", "unknownregion")
    client = MonitoringClient(config)

    # Resolver compartment por nombre u OCID
    identity = oci.identity.IdentityClient(config)
    compartment_id = resolve_compartment_id(identity, config["tenancy"], args.compartment_id)

    # Query robusto para Windows/PowerShell
    mql_query = sanitize_query(args.query)

    # Rango de tiempo + etiqueta
    if args.timeframe:
        start_dt, end_dt, timeframe_label = parse_timeframe(args.timeframe)
    else:
        now = datetime.now(timezone.utc)
        end_dt = iso8601_to_datetime(args.end_time) if args.end_time else now
        start_dt = iso8601_to_datetime(args.start_time) if args.start_time else end_dt - timedelta(hours=1)
        timeframe_label = f"{start_dt.strftime('%m%d%Y_%H%M%S')}-{end_dt.strftime('%m%d%Y_%H%M%S')}"

    # Construye el request
    details = SummarizeMetricsDataDetails(
        namespace=args.namespace,        # <- va en el body
        query=mql_query,                 # <- MQL tal cual (sin comillas exteriores)
        start_time=start_dt,
        end_time=end_dt,
        resolution=args.resolution,
        resource_group=args.resource_group
    )

    if args.debug:
        print(f"[DEBUG] compartment_id = {compartment_id}", file=sys.stderr)
        print(f"[DEBUG] region         = {region}", file=sys.stderr)
        print(f"[DEBUG] namespace      = {args.namespace}", file=sys.stderr)
        print(f"[DEBUG] query          = {mql_query}", file=sys.stderr)
        print(f"[DEBUG] start_time UTC = {start_dt.isoformat()}", file=sys.stderr)
        print(f"[DEBUG] end_time   UTC = {end_dt.isoformat()}", file=sys.stderr)
        if args.resolution:
            print(f"[DEBUG] resolution    = {args.resolution}", file=sys.stderr)
        if args.resource_group:
            print(f"[DEBUG] resource_group= {args.resource_group}", file=sys.stderr)

    # Llama a la API
    resp = client.summarize_metrics_data(
        compartment_id=compartment_id,
        summarize_metrics_data_details=details
    )
    data = resp.data or []

    # -----------------------
    # CSV a stdout
    # -----------------------
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(["series_name", "dimensions", "timestamp", "value"])
    for series in data:
        name = getattr(series, "name", "")
        dims = getattr(series, "dimensions", {}) or {}
        dims_json = json.dumps(dims, ensure_ascii=False, separators=(",", ":"))
        for dp in (series.aggregated_datapoints or []):
            ts = dp.timestamp.isoformat() if dp.timestamp else ""
            w.writerow([name, dims_json, ts, dp.value])

    # -----------------------
    # Guardar JSON a archivo
    # -----------------------
    lb_name = extract_lb_name_from_query(mql_query)
    out_path = build_output_path(args.out_dir, lb_name, region, timeframe_label)

    out_json = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "compartment_id": compartment_id,
        "namespace": args.namespace,
        "query": mql_query,
        "lb_name": lb_name,
        "timeframe_input": args.timeframe if args.timeframe else None,
        "start_time_utc": start_dt.isoformat(),
        "end_time_utc": end_dt.isoformat(),
        "resolution": args.resolution,
        "resource_group": args.resource_group,
        "series_count": len(data),
        "series": []
    }

    for series in data:
        out_json["series"].append({
            "name": getattr(series, "name", ""),
            "dimensions": getattr(series, "dimensions", {}) or {},
            "metadata": getattr(series, "metadata", {}) or {},
            "namespace": getattr(series, "namespace", None),
            "resource_group": getattr(series, "resource_group", None),
            "datapoints": [
                {
                    "timestamp": dp.timestamp.isoformat() if dp.timestamp else None,
                    "value": dp.value
                }
                for dp in (series.aggregated_datapoints or [])
            ]
        })

    # Asegura directorio y guarda
    os.makedirs(args.out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    print(f"\n[INFO] JSON guardado en: {out_path}", file=sys.stderr)

if __name__ == "__main__":
    main()
