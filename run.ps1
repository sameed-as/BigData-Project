# PowerShell runner for Chicago Crime Analytics project on Windows
# Use this instead of the Makefile

param (
    [Parameter(Mandatory=$true)]
    [ValidateSet("up", "down", "batch-analytics", "stream-pipeline", "start-producer", "dashboard", "install-libs", "check-db", "check-mongo", "clean", "help")]
    $Action
)

function Show-Help {
    Write-Host "Available actions:" -ForegroundColor Cyan
    Write-Host "  .\run.ps1 up               - Start all infrastructure containers"
    Write-Host "  .\run.ps1 down             - Stop and remove all containers"
    Write-Host "  .\run.ps1 batch-analytics  - Run the Spark batch analytics job"
    Write-Host "  .\run.ps1 stream-pipeline  - Run the Storm streaming pipeline"
    Write-Host "  .\run.ps1 start-producer   - Start the Kafka producer"
    Write-Host "  .\run.ps1 dashboard        - Start the Streamlit dashboard"
    Write-Host "  .\run.ps1 check-db         - Query Postgres to see batch results"
    Write-Host "  .\run.ps1 check-mongo      - Query MongoDB to see real-time alerts"
    Write-Host "  .\run.ps1 install-libs     - Install required libraries in Spark container"
    Write-Host "  .\run.ps1 clean            - Cleanup Docker system"
}

switch ($Action) {
    "help" {
        Show-Help
    }
    "up" {
        docker-compose -f docker/docker-compose.yml up -d --build
    }
    "down" {
        docker-compose -f docker/docker-compose.yml down
    }
    "batch-analytics" {
        docker exec -it spark /opt/spark/bin/spark-submit --driver-memory 4g --executor-memory 4g --packages org.postgresql:postgresql:42.5.4 /opt/spark-apps/analytics.py
    }
    "stream-pipeline" {
        docker exec -it -e LEIN_ROOT=yes runner bash -c "cd /app/storm && sparse submit -f -e prod"
    }
    "start-producer" {
        docker exec -it runner python3 /app/kafka/producer.py
    }
    "dashboard" {
        docker exec -it -e IN_DOCKER=1 runner streamlit run /app/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
    }
    "check-db" {
        docker exec -it postgres psql -U admin -d crime_db -c "SELECT * FROM crime_trends LIMIT 5; SELECT * FROM top_10_arrest_rates;"
    }
    "check-mongo" {
        docker exec -it mongo mongosh crime_db --eval "db.alert_logs.find().sort({timestamp: -1}).limit(5)"
    }
    "install-libs" {
        docker exec -it spark pip install numpy
    }
    "clean" {
        docker system prune -f
    }
}
