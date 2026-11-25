#!/usr/bin/env python3
"""
ETL Data Pipeline & Analytics Warehouse Orchestrator
Entry point for running the end-to-end data pipeline or individual stages:
Generate -> Extract -> Transform -> Validate -> Load -> Analytics
"""

import os
import sys
import uuid
import argparse
import yaml
from datetime import datetime

from config.logging_config import setup_logger
from src.generator import generate_mock_data
from src.s3_utils import S3Manager
from src.extract import DataExtractor, EXPECTED_FILES
from src.transform import DataTransformer
from src.validate import DataValidator
from src.load import WarehouseLoader
from src.analytics_runner import AnalyticsRunner


def load_env_file(env_path: str = ".env") -> None:
    """Loads key-value pairs from .env into os.environ if not already present."""
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("\"'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Loads configuration YAML and overrides with active environment variables."""
    load_env_file()
    cfg = {}
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # Ensure required section dicts exist
    cfg.setdefault("database", {})
    cfg["database"].setdefault("postgres", {})
    cfg.setdefault("paths", {})
    cfg.setdefault("aws", {})

    # Apply environment variable overrides
    if "DB_TYPE" in os.environ:
        cfg["database"]["type"] = os.environ["DB_TYPE"]
    if "DB_HOST" in os.environ:
        cfg["database"]["postgres"]["host"] = os.environ["DB_HOST"]
    if "DB_PORT" in os.environ:
        cfg["database"]["postgres"]["port"] = int(os.environ["DB_PORT"])
    if "DB_NAME" in os.environ:
        cfg["database"]["postgres"]["dbname"] = os.environ["DB_NAME"]
    if "DB_USER" in os.environ:
        cfg["database"]["postgres"]["user"] = os.environ["DB_USER"]
    if "DB_PASSWORD" in os.environ:
        cfg["database"]["postgres"]["password"] = os.environ["DB_PASSWORD"]
    if "SQLITE_DB_PATH" in os.environ:
        cfg["paths"]["sqlite_db_path"] = os.environ["SQLITE_DB_PATH"]
    if "S3_BUCKET_NAME" in os.environ:
        cfg["aws"]["bucket_name"] = os.environ["S3_BUCKET_NAME"]
    if "AWS_DEFAULT_REGION" in os.environ:
        cfg["aws"]["region"] = os.environ["AWS_DEFAULT_REGION"]
    if "USE_S3_MOCK" in os.environ:
        cfg["aws"]["use_mock"] = os.environ["USE_S3_MOCK"].lower() in ("true", "1", "yes")

    return cfg


def main():
    parser = argparse.ArgumentParser(description="ETL Data Pipeline & Analytics Warehouse")
    parser.add_argument("--full-run", action="store_true", help="Execute complete pipeline end-to-end")
    parser.add_argument("--generate", action="store_true", help="Generate synthetic raw datasets (>10K rows)")
    parser.add_argument("--extract", action="store_true", help="Extract raw data files")
    parser.add_argument("--transform", action="store_true", help="Clean & transform data into star-schema")
    parser.add_argument("--validate", action="store_true", help="Run data validation & quarantine bad records")
    parser.add_argument("--load", action="store_true", help="Load star-schema into warehouse")
    parser.add_argument("--analytics", action="store_true", help="Execute analytical business queries")
    parser.add_argument("--db", type=str, choices=["sqlite", "postgresql"], default=None, help="Target database type")
    parser.add_argument("--s3-sync", action="store_true", help="Sync raw files with AWS S3 / mock bucket")
    parser.add_argument("--records", type=int, default=6000, help="Number of orders to generate (produces >10K items)")

    args = parser.parse_args()

    # Determine if any selective stage was explicitly requested
    selective_flags = [args.generate, args.extract, args.transform, args.validate, args.load, args.analytics]
    is_selective = any(selective_flags) and not args.full_run

    config = load_config()
    batch_id = f"BATCH-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

    logger = setup_logger(
        name="ETL_Pipeline",
        log_file=config.get("paths", {}).get("log_file", "logs/etl.log")
    )

    logger.info("*" * 75)
    mode_str = "SELECTIVE STAGE EXECUTION" if is_selective else "FULL PIPELINE EXECUTION"
    logger.info(f"STARTING ETL PIPELINE - {mode_str} (Batch ID: {batch_id})")
    logger.info("*" * 75)

    raw_dir = config.get("paths", {}).get("raw_dir", "data/raw")
    processed_dir = config.get("paths", {}).get("processed_dir", "data/processed")
    rejected_dir = config.get("paths", {}).get("rejected_dir", "data/rejected")
    db_type = args.db or config.get("database", {}).get("type", "sqlite")
    sqlite_path = config.get("paths", {}).get("sqlite_db_path", "data/analytics_warehouse.db")
    pg_config = config.get("database", {}).get("postgres", {})

    aws_cfg = config.get("aws", {})
    s3_manager = S3Manager(
        bucket_name=aws_cfg.get("bucket_name", "my-ecommerce-data-pipeline-bucket"),
        region=aws_cfg.get("region", "us-east-1"),
        use_mock=aws_cfg.get("use_mock", True)
    )

    # ---------------------------------------------------------
    # STAGE 1: GENERATE
    # ---------------------------------------------------------
    if args.generate or not is_selective:
        logger.info(f"Generating synthetic enterprise datasets (Target: {args.records} orders -> ~12,000 items)...")
        stats = generate_mock_data(output_dir=raw_dir, num_orders=args.records)
        logger.info("Dataset generated:")
        for k, v in stats.items():
            if k != "paths":
                logger.info(f"  - {k.capitalize()}: {v:,} records")
        if is_selective and not any([args.extract, args.transform, args.validate, args.load, args.analytics]):
            logger.info("Stage 'generate' complete. Exiting.")
            return

    # ---------------------------------------------------------
    # STAGE 2: S3 SYNC (if requested or in full run with mock S3)
    # ---------------------------------------------------------
    if args.s3_sync or (not is_selective and aws_cfg.get("use_mock", True)):
        if os.path.exists(raw_dir):
            logger.info("Syncing raw files to S3 bucket...")
            for fname in os.listdir(raw_dir):
                if fname.endswith(".csv"):
                    s3_manager.upload_file(os.path.join(raw_dir, fname), f"raw/{fname}")

    # ---------------------------------------------------------
    # STAGE 3: EXTRACT
    # ---------------------------------------------------------
    raw_dfs = None
    extract_stats = {}
    if args.extract or args.transform or not is_selective:
        extractor = DataExtractor(raw_dir=raw_dir, s3_manager=s3_manager if args.s3_sync else None)
        raw_dfs, extract_stats = extractor.extract_all()
        if is_selective and args.extract and not any([args.transform, args.validate, args.load, args.analytics]):
            logger.info("Stage 'extract' complete. Exiting.")
            return

    # ---------------------------------------------------------
    # STAGE 4: TRANSFORM
    # ---------------------------------------------------------
    transformed_tables = None
    if args.transform or not is_selective:
        if raw_dfs is None:
            extractor = DataExtractor(raw_dir=raw_dir)
            raw_dfs, extract_stats = extractor.extract_all()
        transformer = DataTransformer(processed_dir=processed_dir, batch_id=batch_id)
        transformed_tables = transformer.transform_all(raw_dfs)
        if is_selective and args.transform and not any([args.validate, args.load, args.analytics]):
            logger.info("Stage 'transform' complete. Processed tables written to data/processed. Exiting.")
            return

    # ---------------------------------------------------------
    # STAGE 5: VALIDATE
    # ---------------------------------------------------------
    validated_tables = None
    if args.validate or args.load or not is_selective:
        if transformed_tables is None:
            # Load processed tables from data/processed
            import pandas as pd
            table_names = ["dim_customer", "dim_product", "dim_store", "dim_date", "dim_payment_method", "fact_sales"]
            loaded_proc = {}
            for t in table_names:
                fpath = os.path.join(processed_dir, f"{t}.csv")
                if os.path.exists(fpath):
                    loaded_proc[t] = pd.read_csv(fpath)
                else:
                    logger.warning(f"Processed file {fpath} not found. Running extraction & transformation first...")
                    extractor = DataExtractor(raw_dir=raw_dir)
                    raw_dfs, extract_stats = extractor.extract_all()
                    transformer = DataTransformer(processed_dir=processed_dir, batch_id=batch_id)
                    loaded_proc = transformer.transform_all(raw_dfs)
                    break
            transformed_tables = loaded_proc

        validator = DataValidator(rejected_dir=rejected_dir, batch_id=batch_id)
        validated_tables, val_report = validator.validate_warehouse_dataset(transformed_tables)

        if val_report.get("fatal_errors", 0) > 0:
            logger.critical(f"Pipeline halted: {val_report['fatal_errors']} fatal schema errors detected!")
            sys.exit(1)

        if is_selective and args.validate and not any([args.load, args.analytics]):
            logger.info(f"Stage 'validate' complete. Quarantined: {val_report.get('records_quarantined', 0)}. Exiting.")
            return

    # ---------------------------------------------------------
    # STAGE 6: LOAD
    # ---------------------------------------------------------
    if args.load or not is_selective:
        total_extracted = sum(extract_stats.values()) if extract_stats else len(validated_tables.get("fact_sales", []))
        loader = WarehouseLoader(
            db_type=db_type,
            sqlite_path=sqlite_path,
            pg_config=pg_config,
            batch_id=batch_id
        )
        loader.initialize_schema()
        loader.apply_indexes()
        load_summary = loader.load_warehouse(validated_tables, total_extracted=total_extracted)

        if is_selective and args.load and not args.analytics:
            logger.info(f"Stage 'load' complete. Loaded {load_summary['loaded_counts'].get('fact_sales', 0):,} records into {db_type.upper()}. Exiting.")
            return

    # ---------------------------------------------------------
    # STAGE 7: ANALYTICS & METRICS
    # ---------------------------------------------------------
    if args.analytics or not is_selective:
        analytics = AnalyticsRunner(db_type=db_type, db_path=sqlite_path, pg_config=pg_config)
        analytics.run_all_metrics()
        if is_selective and args.analytics:
            logger.info("Stage 'analytics' complete. Exiting.")
            return

    logger.info("*" * 75)
    logger.info(f"ETL PIPELINE BATCH '{batch_id}' COMPLETED SUCCESSFULLY!")
    logger.info(f"Target Warehouse: {db_type.upper()}")
    logger.info("*" * 75)


if __name__ == "__main__":
    main()
