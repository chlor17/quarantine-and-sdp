# Configuration Setup Guide

This guide explains how to configure the Quarantine and SDP Pipeline for your Databricks environment.

## Quick Start

The repository includes template configuration files with placeholder values. You need to update these with your actual Databricks environment details.

### Method 1: Direct Configuration (Simple)

Edit the configuration files directly with your values:

1. **databricks.yml** - Update the following values:
   - `catalog`: Replace `your_catalog` with your Unity Catalog name
   - `schema`: Replace `your_schema` with your target schema name
   - `workspace.root_path`: Update the path to your workspace location

2. **config.yaml** - Update:
   - `Catalog`: Your Unity Catalog name
   - `Schema`: Your target schema name

### Method 2: Local Override Files (Recommended)

Create local configuration files that override the defaults. These files are gitignored and won't be committed:

1. **Create `config.local.yaml`**:
```yaml
Catalog: "your_actual_catalog"
Schema: "your_actual_schema"

Vol_Partial: "parquet_partial"
Vol_Full: "parquet_full"

Bronze_table: "bronze_table"
Quarantine_table: "quarantine_table"
Dead_letter_table: "dead_letter_table"
Silver_table: "silver_table"

# Quality Control Settings
Quarantine_Hours: 24
Required_Columns: ["id", "name", "age"]
Nullable_Columns: []
```

2. **Update your code** to load `config.local.yaml` if it exists, otherwise fall back to `config.yaml`.

## Configuration Values to Update

### Required Values

| Configuration | File | Description | Example |
|--------------|------|-------------|---------|
| `catalog` | databricks.yml | Unity Catalog name | `my_catalog` |
| `schema` | databricks.yml | Schema for pipeline tables | `data_pipeline` |
| `Catalog` | config.yaml | Unity Catalog name | `my_catalog` |
| `Schema` | config.yaml | Target schema | `data_pipeline` |
| `root_path` | databricks.yml (targets) | Workspace path | `/Workspace/Users/me@company.com/pipeline` |

### Optional Values

| Configuration | File | Description | Default |
|--------------|------|-------------|---------|
| `volume_full` | databricks.yml | Full load volume name | `parquet_full` |
| `volume_partial` | databricks.yml | Partial load volume name | `parquet_partial` |
| `bronze_table` | databricks.yml | Bronze table name | `bronze_table` |
| `quarantine_table` | databricks.yml | Quarantine table name | `quarantine_table` |
| `dead_letter_table` | databricks.yml | Dead letter table name | `dead_letter_table` |
| `silver_table` | databricks.yml | Silver table name | `silver_table` |

## Target Environments

The pipeline includes three target environments:

### demo (default)
- **Mode**: development
- **Use**: Testing and demos
- **Root Path**: Uses dynamic path based on current user

### dev
- **Mode**: development
- **Use**: Development environment
- **Schema suffix**: `_dev`

### prod
- **Mode**: production
- **Use**: Production deployment
- **Schema**: Uses base schema name

## Databricks Asset Bundle Commands

After configuring, deploy using:

```bash
# Validate configuration
databricks bundle validate -t demo

# Deploy to demo environment
databricks bundle deploy -t demo

# Deploy to dev environment
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

## Dynamic Path Variables

The `databricks.yml` uses Databricks variables:

- `${workspace.current_user.userName}` - Automatically resolves to the current user's email/username

This ensures the workspace path is always relative to the deploying user.

## Security Best Practices

1. **Never commit** actual catalog/schema names if they contain sensitive information
2. **Use local override files** (`*.local.yaml`, `*.local.yml`) for environment-specific values
3. **Leverage Databricks profiles** for different workspaces/accounts
4. **Use Databricks Secrets** for sensitive credentials (not configuration files)

## Troubleshooting

### Issue: Bundle validation fails

**Solution**: Check that:
- Your Databricks CLI is authenticated (`databricks auth login`)
- The profile specified exists (`databricks profiles list`)
- Catalog and schema names are valid

### Issue: Access denied to catalog/schema

**Solution**: Ensure your Databricks user/service principal has:
- `USE CATALOG` privilege on the catalog
- `USE SCHEMA` and `CREATE TABLE` privileges on the schema

### Issue: Volume not found

**Solution**: Create volumes manually or add volume creation to your pipeline:
```sql
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name};
```

## Example Setup

For a user `john.doe@company.com` deploying to catalog `analytics` with schema `pipeline_dev`:

**databricks.yml** (targets section):
```yaml
targets:
  dev:
    mode: development
    workspace:
      profile: DEFAULT
      root_path: /Workspace/Users/john.doe@company.com/quarantine-pipeline
    variables:
      catalog: analytics
      schema: pipeline_dev
```

**config.yaml**:
```yaml
Catalog: "analytics"
Schema: "pipeline_dev"
```

Or use **config.local.yaml** (gitignored):
```yaml
Catalog: "analytics"
Schema: "pipeline_dev"
```
