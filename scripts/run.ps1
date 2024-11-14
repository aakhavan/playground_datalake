# Filename: run.ps1

# Create /data/ and /exports/ directories if they do not already exist
if (-Not (Test-Path -Path "./data")) {
    New-Item -ItemType Directory -Path "./data"
    Write-Host "Created /data/ directory."
} else {
    Write-Host "/data/ directory already exists. Skipping creation."
}

if (-Not (Test-Path -Path "./exports")) {
    New-Item -ItemType Directory -Path "./exports"
    Write-Host "Created /exports/ directory."
} else {
    Write-Host "/exports/ directory already exists. Skipping creation."
}

# Define the path to the reviews.csv file
$reviewsCsvPath = "./data/reviews.csv"
$listingsCsvPath = "./data/listings.csv"
$hostsCsvPath = "./data/hosts.csv"

# Function to download a file from S3 if it doesn't exist
function Download-FileIfNotExists {
    param (
        [string]$filePath,
        [string]$s3Path
    )
    if (-Not (Test-Path $filePath)) {
        Write-Host "$filePath not found. Downloading from S3..."
        aws s3 cp $s3Path $filePath
    } else {
        Write-Host "$filePath already exists. Skipping download."
    }
}

# Download the data from S3
Download-FileIfNotExists -filePath $reviewsCsvPath -s3Path "s3://dbtlearn/reviews.csv"
Download-FileIfNotExists -filePath $listingsCsvPath -s3Path "s3://dbtlearn/listings.csv"
Download-FileIfNotExists -filePath $hostsCsvPath -s3Path "s3://dbtlearn/hosts.csv"


# Install dependencies using Poetry
try {
    Write-Host "Installing dependencies with Poetry..."
    poetry install
    Write-Host "Dependencies installed successfully."
} catch {
    Write-Host "Error installing dependencies with Poetry: $_"
    exit 1
}

# Start Docker Compose
try {
    docker-compose up -d -ErrorAction Stop
    Write-Host "Docker Compose started successfully."
} catch {
    Write-Host "Error starting Docker Compose: $_"
    exit 1
}

# Check if data is already loaded
$dataLoaded = docker exec -i postgres-dwh psql -U airbnb -d airbnb -c "SELECT COUNT(*) FROM raw_listings;" | Select-String -Pattern "0 rows"

if ($dataLoaded) {
    Write-Host "Data already loaded. Skipping data import."
} else {
    # Run the setup_dbt_user.sql script inside the PostgreSQL container
    try {
        Write-Host "Running setup_dbt_user.sql script..."
        docker exec -i postgres-dwh psql -U airbnb -d airbnb -f /docker-entrypoint-initdb.d/setup_dbt_user.sql
        Write-Host "setup_dbt_user.sql script executed successfully."
    } catch {
        Write-Host "Error running setup_dbt_user.sql script: $_"
        exit 1
    }
}