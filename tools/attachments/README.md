# Análisis automático de imágenes y documentos

Este servicio descarga los adjuntos disponibles, extrae su contenido y lo incorpora a la búsqueda del historial. Conserva el mensaje original: el resultado se guarda por separado, asociado al mensaje y al archivo que se procesó.

## Qué procesa

- Imágenes y stickers: OCR local y, al configurar OpenAI, interpretación visual con GPT-5.4 mini. También permite descripción local con BLIP. La descripción es una interpretación de IA y puede equivocarse.
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

La extracción de texto y OCR se hace localmente. Con el backend OpenAI, las imágenes y bloques de texto se envían a su API para analizarlos; con BLIP, la descripción visual es local. El texto resultante sigue la configuración de embeddings de tu índice: si elegiste un proveedor externo, ese proveedor recibirá el texto para indexarlo.

## Análisis con OpenAI

Configura estas variables en el servicio. La clave se lee de un archivo privado, nunca se incluye en el repositorio:

```bash
export WA_ATTACHMENT_BACKEND=openai
: "${WA_OPENAI_KEY_FILE:?Define la ruta absoluta del archivo privado con tu clave OpenAI}"
export WA_OPENAI_KEY_FILE
export WA_OPENAI_MODEL=gpt-5.4-mini-2026-03-17
export WA_ATTACHMENT_MONTHLY_USD=10
python -m tools.attachments.worker
```

Este modo no carga BLIP ni requiere sus dependencias. Conserva el texto extraído y añade una interpretación identificada como IA. Analiza todos los bloques de texto compatibles, cada página PDF hasta el límite de 500 y las imágenes raster incrustadas de Office. No renderiza gráficos vectoriales de Office. Procesa hasta ocho unidades por turno y continúa después; un documento pendiente aparece como parcial, no como terminado. Una animación conserva el límite del primer fotograma.

Los resultados se guardan por contenido, modelo y versión de instrucciones; repetir el mismo análisis usa la caché. Antes de cada petición se comprueba que el mensaje y su archivo siguen vigentes y que el grupo está autorizado. Las peticiones usan `store: false`; esto no equivale a una garantía de retención cero por parte del proveedor.

El presupuesto predeterminado es US$10 por mes UTC, separado del gasto de embeddings. Se reservan costos conservadores antes de enviar; al alcanzar el presupuesto, se conserva el texto local y se pospone el análisis al mes siguiente. Las peticiones cuyo resultado se perdió mantienen su reserva para no subestimar el gasto. Tras tres intentos de una misma unidad en un mes, espera al mes siguiente; los intentos bloqueados antes del envío por falta de permiso no cuentan. La caché y el registro de uso permanecen en el almacén privado de WhatsApp.

Tarifas usadas para este modelo: US$0.75 por millón de tokens de entrada y US$4.50 por millón de salida. Son una estimación del consumo de la API, no una factura; consulta las [tarifas oficiales](https://developers.openai.com/api/docs/models/gpt-5.4-mini). Los resultados limitados por la salida del modelo se identifican como parciales.

## Grupos: autorización individual o regla por tamaño

Sin configuración, los grupos están desactivados. Puedes autorizar cada grupo o activar la regla automática de hasta 10 integrantes. Pertenecer a un grupo, recibir mensajes o pedir un resumen no cambia la configuración. El administrador de esta instalación registra una autorización explícita para cada grupo; esta acción no acredita por sí sola consentimiento de todos sus participantes.

```bash
: "${GROUP_JID:?Define el identificador exacto del grupo que autorizas}"
python -m tools.attachments.consent allow --group "$GROUP_JID" --confirm \
  --evidence 'Autorización explícita del responsable de la cuenta, registrada para este grupo'
python -m tools.attachments.consent list
python -m tools.attachments.consent revoke --group "$GROUP_JID" --evidence 'Autorización revocada'
```

La autorización afecta recepción automática, sincronización histórica, transcripción, análisis de adjuntos e indexación. La revocación bloquea nueva captura y procesamiento, y el siguiente ciclo del índice bloquea las búsquedas del grupo. Una limpieza por lotes retira sus mensajes del índice y elimina los embeddings que ya no estén asociados a otro mensaje. El historial bruto que ya existía en el almacén de WhatsApp se conserva; la revocación no borra conversaciones en el teléfono. No concede acceso retroactivo a mensajes que WhatsApp ya no entregue.

Para habilitar la regla automática, ejecuta `python -m tools.attachments.consent auto-enable`; `auto-disable` la apaga e `inventory` muestra los grupos y sus conteos. El servidor recupera metadatos de todos los grupos unidos al conectarse y cada cinco minutos. Sólo autoriza automáticamente conteos conocidos de 1 a 10; una decisión manual `allow` o `revoke` prevalece. Los eventos de membresía invalidan permisos automáticos hasta verificar de nuevo el conteo. Si no se renueva la información, el permiso automático caduca a los 15 minutos; el índice tiene además una vigencia máxima de 60 segundos sin sincronización de permisos. Recuperar metadatos no garantiza que WhatsApp vuelva a entregar todos los mensajes antiguos.

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
