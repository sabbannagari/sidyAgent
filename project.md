# Modular ML Pipeline Project Specification

## Overview
A modular, scalable ML pipeline project in Python supporting multi-cloud deployment (AWS, GCP, Azure) with MLflow tracking, environment-based promotion, and comprehensive metrics logging.

## Project Structure

```
project_name/
├── config/
│   ├── config.json
│   └── project_parser_config.json
├── training/
│   └── train_pipeline.py
├── utils/
│   ├── cloud_storage/
│   │   ├── __init__.py
│   │   ├── aws.py
│   │   ├── gcp.py
│   │   └── azure.py
│   ├── deployment/
│   │   ├── __init__.py
│   │   ├── aws.py
│   │   ├── gcp.py
│   │   └── azure.py
│   ├── __init__.py
│   ├── fetch_data.py
│   ├── train_model.py
│   ├── get_model.py
│   ├── push_model.py
│   ├── deploy_model.py
│   └── metrics_logger.py
└── requirements.txt
```

## Configuration Files

### config/config.json
```json
{
  "cloud_vendor": "aws",
  "model_type": "neural_network",
  "features": ["feature1", "feature2", "feature3"],
  "target": "target_column",
  "train_split": 0.8,
  "training_params": {
    "batch_size": 32,
    "epochs": 10,
    "hidden_size": 128,
    "learning_rate": 0.001
  },
  "model_artifact_path": "models/",
  "endpoint_name": "ml-model-endpoint",
  "promotion": {
    "dev": {
      "s3_bucket": "ml-pipeline-dev",
      "s3_raw_path": "data/raw/",
      "s3_processed_path": "data/processed/",
      "sagemaker": {
        "instance_type": "ml.t2.medium",
        "region": "us-east-1",
        "container_image": "ml-container:dev"
      }
    },
    "qa": {
      "s3_bucket": "ml-pipeline-qa",
      "s3_raw_path": "data/raw/",
      "s3_processed_path": "data/processed/",
      "sagemaker": {
        "instance_type": "ml.m5.large",
        "region": "us-east-1",
        "container_image": "ml-container:qa"
      }
    },
    "staging": {
      "s3_bucket": "ml-pipeline-staging",
      "s3_raw_path": "data/raw/",
      "s3_processed_path": "data/processed/",
      "sagemaker": {
        "instance_type": "ml.m5.xlarge",
        "region": "us-east-1",
        "container_image": "ml-container:staging"
      }
    },
    "prod": {
      "s3_bucket": "ml-pipeline-prod",
      "s3_raw_path": "data/raw/",
      "s3_processed_path": "data/processed/",
      "sagemaker": {
        "instance_type": "ml.m5.2xlarge",
        "region": "us-east-1",
        "container_image": "ml-container:prod"
      }
    }
  }
}
```

### config/project_parser_config.json
```json
{
  "selected_fields": [
    "feature1",
    "feature2",
    "feature3",
    "target_column",
    "timestamp"
  ],
  "field_mappings": {
    "original_feature1": "feature1",
    "original_feature2": "feature2",
    "original_feature3": "feature3"
  }
}
```

## Data Ingestion Flow

1. **Fetch raw data from API** → Save to `s3_raw_path`
2. **Filter fields** using `project_parser_config.json`
3. **Save processed CSV** to `s3_processed_path`

## Training Pipeline Flow

1. **Load processed CSV** from S3 based on environment (dev/qa/staging/prod)
2. **Train model** using `TrainModel` class
3. **Log metrics** (loss, CPU, memory, GPU) using `MetricsLogger`
4. **Push model** to MLflow using `PushModel` class
5. **Deploy model** using `DeployModel` class

## Utility Modules

### Cloud Storage Modules
- `utils/cloud_storage/aws.py`: S3 operations (save_csv, load_csv)
- `utils/cloud_storage/gcp.py`: GCS operations
- `utils/cloud_storage/azure.py`: Azure Blob Storage operations

### Deployment Modules
- `utils/deployment/aws.py`: SageMaker deployment
- `utils/deployment/gcp.py`: Vertex AI deployment
- `utils/deployment/azure.py`: Azure ML deployment

### Core Utilities
- `utils/fetch_data.py`: API data fetching and raw/processed data handling
- `utils/train_model.py`: Model training orchestration
- `utils/get_model.py`: MLflow model retrieval
- `utils/push_model.py`: MLflow model registry operations
- `utils/deploy_model.py`: Multi-cloud deployment orchestration
- `utils/metrics_logger.py`: Continuous metrics logging (CPU, memory, GPU, training metrics)

## MLflow Integration

- **Experiment tracking**: All training runs logged with parameters and metrics
- **Model registry**: Version-controlled model artifacts
- **Model retrieval**: `GetModel` class for loading models
- **Artifact storage**: Model checkpoints and metadata

## Environment Promotion

Environments are configured in `config.json` under the `promotion` key:
- **dev**: Development environment
- **qa**: Quality assurance environment
- **staging**: Pre-production environment
- **prod**: Production environment

Each environment has:
- Dedicated S3 buckets
- Raw and processed data paths
- SageMaker configuration (instance type, region, container image)

## Requirements

### Python Version
- Python 3.9+

### Core Dependencies
- PyTorch: Deep learning framework
- MLflow: Experiment tracking and model registry
- psutil: System metrics (CPU, memory)
- GPUtil: GPU metrics (optional)
- boto3: AWS SDK
- scikit-learn: ML utilities
- pandas: Data manipulation
- requests: API calls

### Cloud SDKs
- boto3 (AWS)
- google-cloud-storage (GCP)
- azure-storage-blob (Azure)

## Key Features

1. **Multi-cloud support**: Abstracted cloud storage and deployment
2. **Environment-based promotion**: Separate configs for dev/qa/staging/prod
3. **Comprehensive metrics logging**: CPU, memory, GPU tracking during training
4. **MLflow integration**: Full experiment tracking and model versioning
5. **Modular design**: Easy to extend and maintain
6. **Configuration-driven**: All settings in JSON configs
7. **Data pipeline**: Raw → Processed → Training flow

## Usage Example

```python
# Set environment
environment = "dev"  # or "qa", "staging", "prod"

# Fetch and process data
fetch_data(environment)

# Train model
train_pipeline(environment)

# Model automatically deployed to configured endpoint
```

## Design Principles

- **Separation of Concerns**: Cloud-specific code isolated in respective modules
- **Configuration over Code**: Behavior driven by config files
- **Extensibility**: Easy to add new cloud providers or model types
- **Observability**: Comprehensive logging and metrics tracking
- **Reproducibility**: MLflow tracking ensures experiment reproducibility
