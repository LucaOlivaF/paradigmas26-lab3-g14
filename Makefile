# ─────────────────────────────────────────────────────────────
# Detectar Java 17 automáticamente según el sistema operativo
# ─────────────────────────────────────────────────────────────

UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
  # macOS: usar java_home para encontrar Java 17
  JAVA_HOME := $(shell /usr/libexec/java_home -v 17 2>/dev/null)
else
  # Linux: buscar la instalación de openjdk-17
  JAVA_HOME := $(shell readlink -f /usr/bin/java 2>/dev/null | sed 's|/bin/java||' || echo "/usr/lib/jvm/java-17-openjdk-amd64")
endif

export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SBT_OPTS := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

# ─────────────────────────────────────────────────────────────
# Argumentos opcionales (se pueden sobreescribir al invocar make)
# Ejemplo: make run SUBSCRIPTION_FILE=data/mis_subs.json TOP_K=20
# ─────────────────────────────────────────────────────────────

SUBSCRIPTION_FILE ?= data/valid_subscriptions.json
ENTITIES_DIR      ?= data/valid_entities
TOP_K             ?= 10

# ─────────────────────────────────────────────────────────────
# Targets
# ─────────────────────────────────────────────────────────────

.PHONY: all run compile clean check-java

## Target principal: compilar y ejecutar con un solo comando
all: run

## Compilar y ejecutar el programa
run: check-java
	sbt "run --subscription-file $(SUBSCRIPTION_FILE) --entities-dir $(ENTITIES_DIR) --top-k $(TOP_K)"

## Solo compilar (sin ejecutar)
compile: check-java
	sbt compile

## Verificar que Java 17 está disponible antes de correr
check-java:
	@if [ -z "$(JAVA_HOME)" ]; then \
		echo "ERROR: No se encontró Java 17."; \
		echo "  Linux: sudo apt install openjdk-17-jdk"; \
		echo "  macOS: brew install openjdk@17"; \
		exit 1; \
	fi
	@echo "Usando Java: $(JAVA_HOME)"
	@$(JAVA_HOME)/bin/java -version 2>&1 | head -1

## Limpiar archivos compilados
clean:
	sbt clean
