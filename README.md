# Quarantine and Spark Declarative Pipelines (SDP)

A production-ready Databricks pipeline implementation featuring robust data quality validation, quarantine processing, and automated data flow orchestration using Spark Declarative Pipelines (SDP).

## Overview

This project demonstrates a comprehensive data pipeline architecture that handles:

- **Full and Partial/Incremental Data Loads**: Support for both complete dataset refreshes and delta processing
- **Multi-Stage Data Quality Validation**: Automated quality checks at bronze and silver layers
- **Quarantine Management**: Intelligent quarantine system with time-based expiry and reprocessing capabilities
- **Dead Letter Queue**: Failed records are captured with detailed error information for analysis
- **Automated Pipeline Orchestration**: Databricks Asset Bundles (DABs) for deployment and job management

## Architecture

The pipeline follows a medallion architecture:

```
Raw Data (Volumes)
    ↓
Bronze Layer (Raw ingestion with metadata)
    ↓
Quality Validation
    ↓
├─→ Silver Layer (Validated data)
├─→ Quarantine Table (Fixable issues, 24hr expiry)
└─→ Dead Letter Queue (Permanent failures)
```

For detailed architecture information, see:
- [PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md) - High-level architecture and design decisions
- [DETAILED_DATA_FLOW.md](DETAILED_DATA_FLOW.md) - Step-by-step data flow with code examples
- [TABLE_SCHEMAS.md](TABLE_SCHEMAS.md) - Complete schema definitions for all tables

## Features

### 🔍 Data Quality Management
- Column-level validation with configurable rules
- Null value checks for required columns
- Schema validation and type checking
- Duplicate detection and handling

### ⏱️ Time-Based Quarantine System
- 24-hour quarantine period (configurable)
- Automatic expiry and movement to dead letter
- Reprocessing capabilities for fixed records
- Comprehensive audit trail

### 📊 Full and Partial Load Support
- **Full Load**: Complete dataset refresh with deduplication
- **Partial Load**: Incremental updates with merge/upsert logic
- Automatic file discovery and processing
- Checkpoint management for exactly-once processing

### 🚀 Production-Ready Components
- Databricks Asset Bundles for CI/CD
- Comprehensive error handling and logging
- Configurable retry mechanisms
- Monitoring and observability hooks

## Project Structure

```
├── src/
│   ├── notebooks/          # Processing notebooks
│   │   ├── full_load.py
│   │   ├── partial_load.py
│   │   ├── quality_check.py
│   │   └── quarantine_processor.py
│   ├── pipelines/          # SDP pipeline definitions
│   │   └── silver_transformation/
│   └── utils/              # Shared utilities
│       ├── config.py
│       ├── quality_validation.py
│       ├── quarantine_ops.py
│       └── schema_definitions.py
├── scripts/                # Data generation and testing scripts
├── resources/              # DAB resource definitions
│   ├── jobs.yml
│   └── pipelines.yml
├── databricks.yml          # Main DAB configuration
├── config.yaml             # Pipeline configuration
└── requirements.txt        # Python dependencies
```

## Getting Started

### Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled
- Databricks CLI installed and configured
- Python 3.9+

### Setup

1. **Clone the repository**:
```bash
git clone https://github.com/chlor17/quarantine-and-sdp.git
cd quarantine-and-sdp
```

2. **Configure your environment**:

   See [CONFIG_SETUP.md](CONFIG_SETUP.md) for detailed configuration instructions.

   Quick configuration:
   - Update `databricks.yml` with your catalog and schema names
   - Update `config.yaml` with your environment-specific values
   - Or create `config.local.yaml` for local overrides (gitignored)

3. **Authenticate with Databricks**:
```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

4. **Validate your configuration**:
```bash
databricks bundle validate -t demo
```

5. **Deploy the pipeline**:
```bash
databricks bundle deploy -t demo
```

### Running the Pipeline

#### Option 1: Via Databricks Workflows (Recommended)

After deployment, your jobs will be available in Databricks:

1. Navigate to **Workflows** in your Databricks workspace
2. Find the deployed jobs (e.g., `full_load_job`, `partial_load_job`)
3. Click **Run Now** or configure a schedule

#### Option 2: Via Databricks CLI

```bash
# Run full load
databricks jobs run-now --job-name "full_load_job"

# Run partial load
databricks jobs run-now --job-name "partial_load_job"

# Run quality check
databricks jobs run-now --job-name "quality_check_job"

# Process quarantine records
databricks jobs run-now --job-name "quarantine_processor_job"
```

#### Option 3: Manual Execution

Open notebooks in Databricks workspace and run interactively:
- `/Workspace/Users/your-email/quarantine-sdp-demo/src/notebooks/full_load.py`

## Configuration

### Key Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `Catalog` | Unity Catalog name | `your_catalog` |
| `Schema` | Target schema | `your_schema` |
| `Vol_Full` | Volume for full loads | `parquet_full` |
| `Vol_Partial` | Volume for partial loads | `parquet_partial` |
| `Quarantine_Hours` | Hours before quarantine expiry | `24` |
| `Required_Columns` | Columns that cannot be null | `["id", "name", "age"]` |

See [config.yaml](config.yaml) for all available options.

## Use Cases

### 1. Initial Data Load (Full Load)
- Load complete dataset from source system
- Validate all records
- Quarantine records with fixable issues
- Move validated data to silver layer

### 2. Incremental Updates (Partial Load)
- Process daily/hourly delta files
- Merge updates with existing data
- Handle deletes and updates
- Maintain data consistency

### 3. Quarantine Processing
- Review quarantined records
- Fix data issues (manual or automated)
- Reprocess fixed records
- Move expired records to dead letter

### 4. Dead Letter Analysis
- Investigate permanently failed records
- Identify systematic data quality issues
- Generate reports for upstream systems

## Documentation

- **[PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md)**: System architecture and design patterns
- **[DETAILED_DATA_FLOW.md](DETAILED_DATA_FLOW.md)**: Step-by-step data flow walkthrough
- **[TABLE_SCHEMAS.md](TABLE_SCHEMAS.md)**: Complete table schema reference
- **[CONFIG_SETUP.md](CONFIG_SETUP.md)**: Environment configuration guide
- **[QUARANTINE_FIX_GUIDE.md](QUARANTINE_FIX_GUIDE.md)**: Guide for handling quarantined records
- **[CLIENT_PRESENTATION_GUIDE.md](CLIENT_PRESENTATION_GUIDE.md)**: Presentation materials and talking points

## Advanced Features

### Custom Quality Validations

Add custom validation logic in `src/utils/quality_validation.py`:

```python
def validate_custom_rule(df: DataFrame) -> DataFrame:
    """Add your custom validation logic"""
    return df.withColumn(
        "is_valid",
        F.when(your_condition, F.lit(True)).otherwise(F.lit(False))
    )
```

### Monitoring and Alerting

The pipeline includes hooks for:
- Quality metrics tracking
- Quarantine volume monitoring
- Processing time alerts
- Data freshness checks

Configure alerts in `resources/jobs.yml` using Databricks alerting.

### Extending the Pipeline

The modular design allows easy extension:
- Add new quality checks
- Implement custom quarantine logic
- Add transformation steps
- Integrate with external systems

## Performance Considerations

- **Optimized for Databricks Runtime**: Uses Delta Lake optimizations
- **Efficient Merges**: Leverages Delta MERGE for upserts
- **Partition Pruning**: Configured for time-based partitioning
- **Auto-optimization**: Enables Delta auto-optimize and auto-compaction

## Troubleshooting

### Common Issues

**Issue**: Quarantine table not found
- **Solution**: Run the initial setup to create all tables, or check catalog/schema names

**Issue**: Access denied errors
- **Solution**: Ensure proper Unity Catalog permissions (USE CATALOG, USE SCHEMA, CREATE TABLE)

**Issue**: Files not being processed
- **Solution**: Check volume paths and file discovery logic in `src/utils/file_discovery.py`

For more help, see the [CONFIG_SETUP.md](CONFIG_SETUP.md) troubleshooting section.

## Contributing

This is a reference implementation. Feel free to adapt it for your specific use cases.

## License

This project is provided as-is for educational and reference purposes.

## Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Spark Declarative Pipelines (SDP)](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Delta Lake](https://delta.io/)

## Contact

For questions or issues related to this implementation, please open an issue on GitHub.

---

**Built with ❤️ using Databricks and Delta Lake**
