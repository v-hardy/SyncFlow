# 🔄 Sync Tool - Sincronización Offline USB ↔ PC

**Herramienta de sincronización offline entre directorios locales y USB con detección de cambios por metadatos y sistema de 3 fases.**

## 🎯 ¿Qué hace el proyecto?

Sync Tool es una herramienta de sincronización diseñada para mantener directorios entre tu PC y un dispositivo USB actualizados de manera offline. Utiliza un sistema de base de datos SQLite para rastrear el estado de los archivos mediante:

- **Hash SHA256**: Identificación única de contenido
- **Metadatos**: Tamaño y timestamp de modificación
- **3 fases de sincronización**: Replicación, detección de cambios y aplicación
- **Resolución de conflictos**: Lógica inteligente para decidir qué versión conservar

El sistema opera completamente offline, ideal para trabajar en múltiples ubicaciones sin conexión constante.

## 🏗️ Arquitectura

| Componente   | Archivo       | Responsabilidad                                              |
| ------------ | ------------- | ------------------------------------------------------------ |
| Schema SQL    | `schema.sql`  | Definición de tablas: master_states, movements, tombstones, history |
| Base de datos | `database.py` | Operaciones CRUD, transacciones, persistencia                |
| Lógica negocio| `domain.py`   | Reglas de validación de movimientos                         |
| Motor sync    | `engine.py`   | Orquestación de las 3 fases de sincronización                |
| Operaciones FS| `fs_util.py`  | Operaciones atómicas de archivos (copiar, mover, borrar)     |
| Metadatos     | `meta_util.py` | Hashing SHA256 y escaneo de directorios                     |
| Simulación    | `dry_run.py`  | Modo de prueba sin modificaciones reales                     |

## 📊 Sistema de 3 Fases

### **FASE 1: Replicación USB → PC**
Compara el estado maestro del USB con el local y sincroniza:

1. Lee `master_states` y `tombstones` del USB
2. Si la PC no tiene master_states: copia todo desde USB
3. Si ambos tienen estados: aplica lógica de resolución de conflictos
4. Actualiza master_states de PC para mantener consistencia

### **FASE 2: Detección de Cambios Locales**
Escanea el filesystem local comparando con master_states:

1. Escanea directorio PC ignorando archivos ocultos (`.sync`)
2. Detecta operaciones: CREATE, MODIFY, MOVE, DELETE
3. Registra movimientos en tabla `movements` de DB temporal
4. Usa hash SHA256 para detectar cambios de contenido

### **FASE 3: Aplicación de Cambios PC → USB**
Aplica los movimientos detectados al USB:

1. Sincroniza master_states desde PC a DB temporal
2. Valida cada movimiento con reglas de negocio
3. Aplica operaciones FS correspondientes
4. Actualiza master_states y archiva movimientos en history

## ⚖️ Lógica de Decisión por Caso

### **CREATE (Archivo Nuevo)**
**Detección**: Archivo existe en filesystem pero no en master_states

**Decisión**:
- Si el hash no existe en ningún registro → Operación CREATE
- Si el hash existe en otro path → Operación MOVE (renombrado)

**Aplicación**:
- Copia archivo de PC → USB
- Registra en master_states con init_hash = content_hash
- Valida que no exista destino en USB antes de copiar

### **MODIFY (Archivo Modificado)**
**Detección**: Archivo existe en ambos lados pero con diferentes:
- Tamaño (size_bytes)
- Timestamp (last_op_time) con tolerancia de 2 segundos
- Hash de contenido (content_hash)

**Decisión**:
- Si hash es diferente → Operación MODIFY
- Si solo cambió timestamp sin cambio de contenido → Ignora (tolerancia)

**Aplicación**:
- Copia versión modificada de PC → USB
- Actualiza content_hash, size_bytes y last_op_time en master_states
- Requiere que archivo exista en master_states

### **MOVE (Archivo Movido/Renombrado)**
**Detección**: Mismo hash de contenido pero diferente ruta relativa

**Decisión**:
- Si hash existe en master_states con path diferente → Operación MOVE
- Valida que origen exista y destino no exista

**Aplicación**:
- Mueve archivo de path origen → destino en USB
- Actualiza rel_path en master_states
- Preserva init_hash (identidad del archivo)

### **DELETE (Archivo Borrado)**
**Detección**: Archivo existe en master_states pero no en filesystem

**Decisión**:
- Si archivo no se encuentra en escaneo → Operación DELETE
- Verifica que archivo exista en master_states

**Aplicación**:
- Borra archivo del USB
- Registra en tabla `tombstones` para prevenir recreación
- Elimina de master_states
- Guarda timestamp de eliminación para auditoría

### **Resolución de Conflictos (FASE 1)**
Cuando el mismo archivo existe en ambos lados con diferentes estados:

**Caso 1: Mismo contenido, diferente path**
- Decisión: Operación MOVE
- Acción: Mover archivo al path del USB

**Caso 2: Diferente contenido, USB más reciente**
- Decisión: Operación UPDATE desde USB
- Acción: Copiar versión USB → PC (USB tiene autoridad)

**Caso 3: Diferente contenido, PC más reciente**
- Decisión: Registrar como movimiento local
- Acción: Se propagará en FASE 3 cuando se reconecte USB

**Caso 4: Archivo solo en PC con tombstone en USB**
- Decisión: Eliminar de PC
- Acción: El archivo fue borrado en otra ubicación

## 🛠️ Detalles de Uso

| Escenario                           | Comando                                                                                                                       | Descripción                                                                                                     |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Uso básico (Windows)**            | `python run_sync.py --pc-root C:/Users/yo/data --usb-root E:/data`                                                            | Ejecuta el sync normal usando paths de PC y USB, DB por defecto (`metadata.db`) y log por defecto (`sync.log`). |
| **Uso básico (Linux/macOS)**        | `python3 run_sync.py --pc-root /home/yo/data --usb-root /media/usb/data`                                                      | Igual que el anterior, adaptado a rutas de Unix.                                                                |
| **Simulación / Dry-run**            | `python run_sync.py --pc-root C:/Users/yo/data --usb-root E:/data --dry-run`                                                  | Simula el sync sin modificar archivos ni la DB.                                                                 |
| **Cambiar nombre de la DB**         | `python run_sync.py --pc-root C:/Users/yo/data --usb-root E:/data --db-name maestro.db`                                       | Usa `maestro.db` en lugar de `metadata.db`.                                                                     |
| **Cambiar archivo de log**          | `python run_sync.py --pc-root C:/Users/yo/data --usb-root E:/data --log logs/sync_2026.log`                                   | Guarda los logs en la ruta especificada.                                                                        |
| **Argumentos extra para `main.py`** | `python run_sync.py --pc-root C:/Users/yo/data --usb-root E:/data --extra-flag1 --extra-flag2`                                | Cualquier flag no reconocido por el lanzador se pasa directamente a `main.py`.                                  |
| **Alias Linux/macOS**               | `alias run_sync="python3 /ruta/a/tu/proyecto/run_sync.py"` <br> `run_sync --pc-root /home/yo/data --usb-root /media/usb/data` | Permite ejecutar el lanzador con un comando corto desde cualquier terminal.                                     |

## 📁 Estructura de Base de Datos

### **master_states** (Estado maestro de archivos)
- `init_hash`: Hash inicial del archivo (identidad permanente)
- `rel_path`: Ruta relativa actual
- `content_hash`: Hash actual del contenido
- `size_bytes`: Tamaño en bytes
- `last_op_time`: Timestamp de última operación
- `machine_name`: Nombre de máquina que realizó la última operación

### **movements** (Cambios pendientes de aplicar)
- `id`: ID auto-incremental
- `op_type`: CREATE/MODIFY/MOVE/DELETE
- `init_hash`: Identidad del archivo
- `rel_path`: Ruta origen
- `new_rel_path`: Ruta destino (solo para MOVE)
- `content_hash`: Hash del contenido
- `size_bytes`: Tamaño
- `last_op_time`: Timestamp
- `machine_name`: Máquina origen

### **tombstones** (Registro de borrados)
- `init_hash`: Identidad del archivo borrado
- `content_hash`: Último hash conocido
- `deleted_at`: Timestamp de eliminación
- `machine_name`: Máquina que realizó el borrado

### **movements_history** (Historial de operaciones)
- Estructura similar a movements + `applied_time`: Timestamp de aplicación

## 🧪 Testing

El proyecto incluye:
- Tests unitarios para cada componente (DB, dominio, FS, metadatos)
- Tests de integración end-to-end
- Tests de dry-run
- Tests para las 3 fases del engine

```bash
# Ejecutar tests (requiere pytest)
python3 -m pytest tests/ -v

# Test de integración manual
python3 tests/test_integration.py
```

## ⚠️ Consideraciones Importantes

1. **Archivos ocultos**: El sistema ignora directorios que comienzan con `.` (como `.sync`)
2. **Tolerancia temporal**: Diferencias menores a 2 segundos en timestamps son ignoradas
3. **Conflicto de autoridad**: En FASE 1, el USB tiene prioridad sobre la PC
4. **Idempotencia**: Las operaciones pueden aplicarse múltiples veces sin efectos adversos
5. **Historial**: Todos los movimientos quedan registrados en `movements_history`

## 🚀 Estado del Proyecto

**✅ Funcionalidades Completadas:**
- Sistema de 3 fases de sincronización
- Schema SQL completo con 4 tablas
- Operaciones CRUD de base de datos
- Lógica de dominio y validación de movimientos
- Operaciones atómicas de filesystem
- Sistema de metadatos y hashing SHA256
- Modo dry-run funcional
- Lanzador multiplataforma
- Tests unitarios e integración

**📈 Próximas mejoras potenciales:**
- Sincronización bidireccional completa
- Soporte para conflictos manuales
- Compresión de archivos grandes
- Sincronización incremental
- Interfaz gráfica opcional

