#!/bin/bash
# Script to create Kubernetes secrets from .env file for NATS Firehose Ingest
# This creates the secret that the Helm chart references via envFrom in the deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
NAMESPACE="default"
SECRET_NAME="nats-firehose-ingest-env"
ENV_FILE="$(dirname "$0")/../.env.prod"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace|-n)
            NAMESPACE="$2"
            shift 2
            ;;
        --secret-name|-s)
            SECRET_NAME="$2"
            shift 2
            ;;
        --env-file|-f)
            ENV_FILE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Create Kubernetes secrets from .env file for NATS Firehose Ingest"
            echo ""
            echo "Options:"
            echo "  -n, --namespace NS    Kubernetes namespace (default: default)"
            echo "  -s, --secret-name NAME Secret name (default: nats-firehose-ingest-env)"
            echo "  -f, --env-file FILE   Path to .env file (default: ./.env)"
            echo "  --dry-run             Show what would be created without creating it"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option $1${NC}"
            exit 1
            ;;
    esac
done

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed or not in PATH${NC}"
    exit 1
fi

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}Error: .env file not found at $ENV_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}Creating Kubernetes secret for NATS Firehose Ingest${NC}"
echo "Namespace: $NAMESPACE"
echo "Secret Name: $SECRET_NAME"
echo "Env File: $ENV_FILE"
echo ""

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo -e "${YELLOW}Warning: Namespace '$NAMESPACE' does not exist${NC}"
    read -p "Do you want to create it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kubectl create namespace "$NAMESPACE"
        echo -e "${GREEN}✓ Created namespace '$NAMESPACE'${NC}"
    else
        echo -e "${RED}Aborted${NC}"
        exit 1
    fi
fi

# Build kubectl command
CMD_ARGS=()

# Read .env file and build --from-literal arguments
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Extract key=value pairs
    if [[ "$line" =~ ^[[:space:]]*([^=]+)=(.*)$ ]]; then
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
        
        # Remove leading/trailing whitespace from key
        key=$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        # Remove quotes from value if present
        value=$(echo "$value" | sed 's/^["'\'']*//;s/["'\'']*$//')
        
        CMD_ARGS+=("--from-literal=${key}=${value}")
        echo "  ✓ $key"
    fi
done < "$ENV_FILE"

if [ ${#CMD_ARGS[@]} -eq 0 ]; then
    echo -e "${RED}Error: No valid environment variables found in $ENV_FILE${NC}"
    exit 1
fi

# Delete existing secret if it exists
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
    echo -e "${YELLOW}Deleting existing secret '$SECRET_NAME'${NC}"
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] kubectl delete secret \"$SECRET_NAME\" -n \"$NAMESPACE\""
    else
        kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
    fi
fi

# Create the secret
echo -e "${YELLOW}Creating secret '$SECRET_NAME' with ${#CMD_ARGS[@]} environment variables${NC}"

if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] kubectl create secret generic \"$SECRET_NAME\" -n \"$NAMESPACE\" \\"
    for arg in "${CMD_ARGS[@]}"; do
        echo "  $arg \\"
    done
    echo ""
    echo -e "${GREEN}✓ Dry run completed successfully${NC}"
else
    kubectl create secret generic "$SECRET_NAME" -n "$NAMESPACE" "${CMD_ARGS[@]}"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Secret '$SECRET_NAME' created successfully${NC}"
        
        # Show secret info
        echo ""
        echo "Secret information:"
        kubectl get secret nats-firehose-ingest-env -o json | jq -r '.data | to_entries[] | "\(.key): \(.value | @base64d)"'
        
        echo ""
        echo "To use this secret in your Helm chart:"
        echo "  helm install nats-firehose-ingest ./nats-firehose-ingest --namespace $NAMESPACE"
        echo ""
        echo "To update the secret, re-run this script with new values in $ENV_FILE"
    else
        echo -e "${RED}✗ Failed to create secret${NC}"
        exit 1
    fi
fi