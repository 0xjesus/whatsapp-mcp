# Análisis automático de imágenes y documentos

Este servicio descarga los adjuntos disponibles, extrae su contenido y lo incorpora a la búsqueda del historial. Conserva el mensaje original: el resultado se guarda por separado, asociado al mensaje y al archivo que se procesó.

## Qué procesa

- Imágenes y stickers: OCR local y, al configurar el modelo, descripción visual local. La descripción es una interpretación de IA y puede equivocarse.
- PDF: texto y OCR de cada página, incluidos documentos escaneados y páginas que mezclan texto e imágenes.
- Word `.docx`: párrafos, tablas, encabezados, pies y notas; OCR de imágenes incrustadas compatibles.
- Excel `.xlsx`: todas las hojas, referencias de celda, valores almacenados y fórmulas. No ejecuta ni recalcula fórmulas.
- PowerPoint `.pptx`: todas las diapositivas y notas del presentador; OCR de imágenes incrustadas compatibles.
- TXT, Markdown, CSV, TSV, JSON, XML y YAML: texto completo dentro del límite configurado en el extractor.

No ejecuta macros, scripts ni instrucciones contenidas en los archivos. Los formatos antiguos `.doc`, `.xls`, `.ppt`, archivos cifrados y formatos desconocidos no se presentan como procesados correctamente. El OCR no garantiza leer texto borroso o manuscrito. No interpreta por completo diagramas o gráficos vectoriales de Office. Las animaciones se marcan como parciales porque se procesa su primer fotograma.

## Instalación

Usa Python 3.10 a 3.12 y un entorno virtual dedicado en el mismo equipo del servidor WhatsApp. Desde la raíz del repositorio:

```bash
python3 -m venv .venv-attachments
. .venv-attachments/bin/activate
python -m pip install -r tools/attachments/requirements.txt
python -m pip install --no-deps rapidocr-onnxruntime==1.4.4
python -m pip install -r tools/attachments/requirements-vision.txt
```

La instalación de OCR usa OpenCV headless, que no requiere un escritorio ni libGL. Sus dependencias están enumeradas en el archivo de requisitos; `--no-deps` evita instalar además la variante gráfica de OpenCV.

La visión utiliza [BLIP image captioning base](https://huggingface.co/Salesforce/blip-image-captioning-base). Descarga sus archivos una vez antes de arrancar. La inferencia exige archivos locales y no descarga código del modelo:

```bash
: "${WA_VISION_MODEL:?Define el directorio absoluto donde guardar el modelo}"
hf download Salesforce/blip-image-captioning-base --local-dir "$WA_VISION_MODEL"
: "${WA_STORE:?Define el directorio absoluto del almacén del servidor WhatsApp}"
export WA_STORE WA_VISION_MODEL
export WA_MCP_URL=http://127.0.0.1:8765/mcp
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false
python -m tools.attachments.worker
```

Sin `WA_VISION_MODEL`, el OCR de imágenes funciona pero su estado indica análisis parcial. Para búsqueda semántica, instala también [el índice de historial](../../docs/history-install.md). Al actualizar una instalación existente, reinicia el índice y su API con esta versión; el usuario SQL de lectura necesita acceso a la nueva tabla `group_monitoring_consent`.

La extracción y la descripción visual se hacen localmente. El texto resultante sigue la configuración de embeddings de tu índice: si elegiste un proveedor externo, ese proveedor recibirá el texto para indexarlo.

## Grupos: autorización explícita

Los grupos están desactivados por defecto. Pertenecer a un grupo, recibir mensajes o pedir un resumen no activa su monitoreo. El administrador de esta instalación registra una autorización explícita para cada grupo; esta acción no acredita por sí sola consentimiento de todos sus participantes.

```bash
: "${GROUP_JID:?Define el identificador exacto del grupo que autorizas}"
python -m tools.attachments.consent allow --group "$GROUP_JID" --confirm \
  --evidence 'Autorización explícita del responsable de la cuenta, registrada para este grupo'
python -m tools.attachments.consent list
python -m tools.attachments.consent revoke --group "$GROUP_JID" --evidence 'Autorización revocada'
```

La autorización afecta recepción automática, sincronización histórica, transcripción, análisis de adjuntos e indexación. La revocación bloquea nueva captura y procesamiento, y el siguiente ciclo del índice bloquea las búsquedas del grupo. Una limpieza por lotes retira sus mensajes del índice y elimina los embeddings que ya no estén asociados a otro mensaje. El historial bruto que ya existía en el almacén de WhatsApp se conserva; la revocación no borra conversaciones en el teléfono. No concede acceso retroactivo a mensajes que WhatsApp ya no entregue.

Las decisiones quedan registradas localmente. No publiques esa base de datos ni las evidencias de consentimiento.

## Estados y límites

El servicio procesa un archivo por vez. Da prioridad a mensajes del último día y avanza por el resto del historial del más antiguo al más reciente. La tabla `attachment_analysis` registra estado, intentos, motivo, método y cobertura.

- `done`: terminó la extracción compatible del archivo; no garantiza una interpretación perfecta.
- `partial`: hay contenido útil, pero falta visión, se alcanzó un límite o sólo se leyó el primer fotograma de una animación. El índice identifica el texto como parcial.
- `failed`: fallo temporal; reintenta con espera, hasta tres intentos.
- `failed_permanent`: agotó intentos o excedió límites.
- `unsupported`: formato no compatible.

Límites actuales: archivos de 50 MiB, 100 MiB de contenido Office descomprimido, 500 páginas PDF, dos millones de caracteres y diez minutos por archivo. Los límites evitan saturar el servidor; nunca se etiqueta una extracción recortada como completa. Los archivos vencidos pueden resultar irrecuperables. Los fallos y formatos no compatibles quedan registrados para revisión.

Ejemplo de consulta de cobertura, ejecutada sobre la base del servidor:

```sql
SELECT status, count(*) FROM attachment_analysis GROUP BY status;
```

Los resultados se unen al texto original para búsqueda; los extractos mostrados por una búsqueda pueden ser más cortos que el texto completo almacenado. Una edición que cambie el archivo invalida el análisis anterior; una eliminación impide publicarlo nuevamente.

## Pruebas

```bash
python -m unittest discover -s tools/attachments -p 'test_*.py'
PYTHONPATH=tools/history python -m unittest discover -s tools/history/tests -p test_sources.py
```

Las pruebas generan contenido sintético. Con las dependencias instaladas verifican OCR real y PDF de varias páginas; con `WA_VISION_MODEL` configurado también ejecutan inferencia visual local. La suite de integración de historial comprueba extracción tardía, búsquedas y autorización/revocación de grupos sobre PostgreSQL aislado.
