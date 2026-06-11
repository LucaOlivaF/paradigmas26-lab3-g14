# paradigmas26-lab3-g14

Laboratorio 3 — Procesamiento distribuido con Apache Spark  
Paradigmas de Programación 2026 — FAMAF

## Descripción

Este programa integra todas las funcionalidades de los laboratorios anteriores y las
ejecuta de forma distribuida usando Apache Spark. Lee suscripciones a subreddits desde
un archivo JSON, descarga los posts en paralelo, extrae entidades nombradas (personas,
organizaciones, lugares, lenguajes de programación, etc.) y muestra estadísticas y
rankings de las entidades más frecuentes.

## Requisitos previos

- **Java 17** (Spark 3.x no es compatible con Java 18+)
- **sbt** (Scala Build Tool)

> Podés tener Java 21/24 instalado globalmente sin problema. El proyecto usa Java 17
> solo para este laboratorio, sin cambiar la configuración global del sistema.

---

## Instalación de dependencias

### Java 17

**Linux (Ubuntu/Debian)**
```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

Para verificar la instalación:
```bash
java -version
```

**macOS (con Homebrew)**
```bash
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

Para verificar que está disponible:
```bash
/usr/libexec/java_home -V
```

**Windows**

Descargar el instalador de [Eclipse Temurin 17](https://adoptium.net/temurin/releases/?version=17)
y seguir el asistente de instalación. Luego, en cada sesión de PowerShell donde se
quiera correr el proyecto, ejecutar:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH
```

### sbt

**Linux (Ubuntu/Debian)**
```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | sudo apt-key add -
sudo apt update
sudo apt install sbt
```

**macOS**
```bash
brew install sbt
```

**Windows**

Descargar el instalador `.msi` desde [scala-sbt.org](https://www.scala-sbt.org/download.html).

---

## Configuración del entorno

Spark requiere flags especiales de Java para funcionar correctamente con Java 17.
Estas opciones ya están configuradas en el archivo `build.sbt` del proyecto:

```scala
fork := true
ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
```

No es necesario modificar nada manualmente.

---

## Estructura del proyecto

```
paradigmas26-lab3-g14/
├── build.sbt
├── Makefile
├── README.md
├── INFORME.md
├── data/
│   ├── valid_subscriptions.json
│   └── valid_entities/
│       ├── people.txt
│       ├── organizations.txt
│       ├── universities.txt
│       ├── places.txt
│       └── languages.txt
└── src/
    └── main/
        └── scala/
            ├── Main.scala
            ├── Analyzer.scala
            ├── CommandLineArgs.scala
            ├── Dictionary.scala
            ├── FileIO.scala
            ├── Formatters.scala
            ├── JsonParser.scala
            ├── NamedEntity.scala
            ├── Post.scala
            └── Subscription.scala
```

---

## Ejecución

### Opción 1 — con Make (recomendado)

```bash
make run
```

### Opción 2 — con sbt directamente

**Linux**
```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
PATH="$JAVA_HOME/bin:$PATH" \
SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
sbt run
```

Si tu distribución instala Java en una ruta distinta, podés encontrarla con:
```bash
update-alternatives --config java
# o bien:
readlink -f $(which java)
```

**macOS**
```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
PATH="$JAVA_HOME/bin:$PATH" \
SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
sbt run
```

**Windows (PowerShell)**
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH
$env:SBT_OPTS = "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
sbt run
```

### Argumentos opcionales

| Argumento | Descripción | Valor por defecto |
|---|---|---|
| `--subscription-file <ruta>` | Archivo JSON de suscripciones | `data/valid_subscriptions.json` |
| `--entities-dir <ruta>` | Directorio de diccionarios de entidades | `data/valid_entities` |
| `--top-k <n>` | Cantidad de entidades top a mostrar | `10` |

Ejemplo:
```bash
sbt "run --subscription-file data/mis_subs.json --top-k 20"
```

---

## Servidor mock local (para testing)

Para evitar consultas reales a Reddit durante el desarrollo, podés usar el servidor
mock incluido. En una terminal separada, desde la carpeta `reddit-mock/`:

```bash
sbt run
```

Deberías ver:
```
Fake Reddit API running on http://localhost:8123
Press Ctrl+C to shut down.
```

El servidor sirve posts estáticos de `r/scala`, `r/programming` y `r/learnpython` en
el puerto `8123`. Para usarlo, cambiá las URLs en tu archivo de suscripciones para
que apunten a `http://localhost:8123`.

---

## Salida esperada

```
============ ESTADÍSTICAS DE PROCESAMIENTO ============
Feeds descargados exitosamente:??? 
Feeds fallidos: ???
Posts descargados exitosamente:???
Posts filtrados (vacíos/nulos):???
Largo promedio en posts:???

============ ESTADÍSTICAS DE ENTIDADES ============
Entidades totales:???
Entidades por categoría:
    [ProgrammingLanguage]:???
    [Person]:???
    ...

============ ENTIDADES NOMBRADAS MÁS FRECUENTES ============
[Type=ProgrammingLanguage] Scala: ??? apariciones
[Type=ProgrammingLanguage] Python: ??? apariciones
...
```

---

## Autores

Grupo 14 — Paradigmas de Programación 2026, FAMAF(Axel Guevara, Fabrizio Reyna, Milena Juarez, Lucas Oliva)
