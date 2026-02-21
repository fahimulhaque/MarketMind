#!/usr/bin/env bash
set -eo pipefail

# Navigation context to ensure it works regardless of where it's called from
cd "$(dirname "$0")"

# Execute with the 'full' profile active to ensure all services are recognized by start, stop, logs, etc.
export COMPOSE_PROFILES=${COMPOSE_PROFILES:-full}

# Colors for UI
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper Functions
log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker is not installed. Please install Docker."
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is not running. Please start Docker."
        exit 1
    fi
}

setup_env() {
    if [ ! -f .env.prod ]; then
        log_info "No .env.prod found. Bootstrapping from .env.example..."
        if [ -f .env.example ]; then
            cp .env.example .env.prod
            # Set redis host assuming it is deployed on host in docker network
            sed -i.bak 's/REDIS_HOST=localhost/REDIS_HOST=host.docker.internal/' .env.prod || true
            rm -f .env.prod.bak
            log_success "Created .env.prod file. Please verify variables (like REDIS_HOST) before restarting!"
        else
            log_error "No .env.example found to bootstrap from."
            exit 1
        fi
    fi
}

start_services() {
    check_docker
    setup_env

    log_info "Starting up production services (this might take a moment to pull images)..."
    
    # --remove-orphans keeps things clean if services were removed from the compose file
    docker compose -f docker-compose.prod.yml up -d --remove-orphans

    log_success "All production services started."
    print_dashboard
}

print_dashboard() {
    echo -e "\n${GREEN}🚀 TickerAgent Production Stack is running!${NC}"
    echo "----------------------------------------------------"
    echo -e "📱 ${BLUE}Dashboard UI:${NC}    http://localhost:3005"
    echo -e "🧠 ${BLUE}API Gateway:${NC}     http://localhost:8080/api/docs"
    echo -e "🗄️  ${BLUE}Database:${NC}        localhost:5432"
    echo -e "💬 ${BLUE}Ollama:${NC}          http://localhost:11434"
    echo "----------------------------------------------------"
    echo "Useful commands:"
    echo "  ./start-prod.sh logs       - View streaming, colorized logs for all services"
    echo "  ./start-prod.sh stop       - Stop services (keeps data)"
    echo "  ./start-prod.sh down       - Stop and remove containers"
    echo "  ./start-prod.sh clean      - Full atomic teardown (DANGER: clear db volumes)"
    echo ""
}

print_help() {
    echo -e "\n${GREEN}🧠 TickerAgent Production CLI${NC}"
    echo -e "An orchestration wrapper for production deployment.\n"
    
    echo -e "${YELLOW}Usage:${NC} ./start-prod.sh [command] [service]"
    echo ""
    echo -e "${YELLOW}Lifecycle Commands:${NC}"
    echo -e "  ${GREEN}start${NC}           - Bootstrap environment and start all services in the background."
    echo -e "  ${GREEN}stop${NC}            - Gracefully stop all services (retains data)."
    echo -e "  ${GREEN}down${NC}            - Spin down containers and networks (retains volumes)."
    echo -e "  ${GREEN}clean${NC}           - Atomic teardown! Destroys containers and DB volumes."
    echo ""
    echo -e "${YELLOW}Granular Control:${NC}"
    echo -e "  ${BLUE}status${NC}          - Show running status and health of the prod stack."
    echo -e "  ${BLUE}logs${NC} [service]  - Tail streaming logs (e.g., './start-prod.sh logs', './start-prod.sh logs api')."
    echo -e "  ${BLUE}restart${NC} [svc]   - Restart the whole stack, or just one piece."
    echo -e "  ${BLUE}refresh${NC} [svc]   - Force rebuild and recreate a container."
    echo -e "  ${BLUE}shell${NC} <service> - Drop into an interactive bash/sh prompt inside a running container."
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  ./start-prod.sh start"
    echo "  ./start-prod.sh logs api"
    echo "  ./start-prod.sh restart worker"
    echo "  ./start-prod.sh shell postgres"
    echo ""
}

# CLI Router
SERVICE=$2

case "$1" in
    start|"")
        start_services
        ;;
    stop)
        log_info "Stopping TickerAgent Production..."
        docker compose -f docker-compose.prod.yml stop $SERVICE
        ;;
    down)
        log_info "Spinning down TickerAgent Production..."
        docker compose -f docker-compose.prod.yml down
        ;;
    status|ps)
        log_info "Container Status:"
        docker compose -f docker-compose.prod.yml ps
        ;;
    logs)
        shift
        docker compose -f docker-compose.prod.yml logs -f "$@"
        ;;
    restart)
        if [ -z "$SERVICE" ]; then
            log_info "Restarting all production services..."
        else
            log_info "Restarting service: $SERVICE..."
        fi
        docker compose -f docker-compose.prod.yml restart $SERVICE
        ;;
    refresh|build)
        log_info "Force rebuilding and recreating containers (Service: ${SERVICE:-All})..."
        docker compose -f docker-compose.prod.yml build --no-cache $SERVICE
        docker compose -f docker-compose.prod.yml up -d --force-recreate $SERVICE
        log_success "Refresh complete."
        ;;
    shell|exec)
        if [ -z "$SERVICE" ]; then
            log_error "You must specify a service to shell into (e.g., './start-prod.sh shell api')."
            exit 1
        fi
        log_info "Dropping into $SERVICE..."
        # Try bash first, fallback to sh if bash isn't installed
        docker compose -f docker-compose.prod.yml exec $SERVICE /bin/sh -c "command -v bash >/dev/null 2>&1 && exec bash || exec sh"
        ;;
    clean)
        echo -e "${YELLOW}WARNING: This will destroy all local data volumes (Postgres, Ollama).${NC}"
        read -p "Are you sure? [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker compose -f docker-compose.prod.yml down -v --remove-orphans --rmi local
            log_success "Cleaned up all prod containers, volumes, and networks."
        else
            echo -e "\nCleanup cancelled."
        fi
        ;;
    help|-h|--help)
        print_help
        ;;
    *)
        log_error "Unknown command: $1"
        print_help
        exit 1
        ;;
esac
