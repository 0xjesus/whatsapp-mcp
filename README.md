# WhatsApp MCP: conversaciones consultables desde tu asistente

[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Go 1.25+](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![MCP](https://img.shields.io/badge/MCP-protocol-6366f1)](https://modelcontextprotocol.io/)
[![whatsmeow](https://img.shields.io/badge/whatsmeow-multidevice-25D366?logo=whatsapp&logoColor=white)](https://github.com/tulir/whatsmeow)

Conecta tu WhatsApp con un asistente de IA para consultar conversaciones, buscar acuerdos y trabajar con mensajes y archivos. Usa MCP, el protocolo que permite al asistente llamar herramientas de otras aplicaciones.

Este fork añade búsqueda por significado, transcripción de audios y videos, y un historial que incorpora reacciones, ediciones y eliminaciones recibidas. La búsqueda semántica y la transcripción necesitan los servicios opcionales incluidos en el repositorio.

Parte de [Sealjay/mcp-whatsapp](https://github.com/Sealjay/mcp-whatsapp), basado a su vez en [lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp). La conexión con WhatsApp utiliza [whatsmeow](https://github.com/tulir/whatsmeow). El crédito por el servidor base corresponde a esos proyectos.

## Lo que añade este fork

<a id="what-this-fork-adds"></a>

- **Buscar por lo que recuerdas.** Encuentra conversaciones por su significado, aunque no recuerdes las palabras exactas. Combina búsqueda semántica y búsqueda de texto.
- **Buscar dentro de notas de voz.** Transcribe los audios disponibles para que puedas consultar lo que se dijo. La transcripción se ejecuta localmente.
- **Revisar videos desde el asistente.** Transcribe su audio y entrega hasta seis fotogramas para inspeccionar el contenido visual. Los fotogramas son una muestra del clip.
- **Recordar reacciones recibidas.** Guarda quién reaccionó a un mensaje y refleja si cambió o quitó el emoji.
- **Mantener el historial actualizado.** Incorpora ediciones y eliminaciones recibidas. Evita que una sincronización antigua vuelva a introducir mensajes revocados.
- **Registrar más tipos de contenido.** Guarda stickers, ubicaciones y contactos. Si llega un tipo todavía no compatible, deja un aviso en el historial.
- **Solicitar más historial antiguo.** Al vincular la cuenta, puede pedir una ventana histórica mayor. La recuperación depende de lo que WhatsApp entregue.
- **Aplicar controles adicionales de envío.** Espacia mensajes, aplica límites y frena acciones cuando detecta restricciones de la cuenta. Estos controles no garantizan evitar bloqueos.
- **Mostrar cuánto falta por procesar.** Permite consultar la cobertura del índice, los mensajes pendientes de preparar para búsquedas y estadísticas del historial disponible.

Leer y enviar mensajes, gestionar grupos, trabajar con encuestas y enviar reacciones ya forman parte del proyecto base. Las mejoras anteriores describen las aportaciones de este fork; no son una comparación con todos los clientes de WhatsApp existentes.

La implementación y sus límites están documentados en las guías de [memoria y búsqueda](docs/memory.md), [transcripción](tools/transcriber/README.md) y [reacciones](docs/reactions.md). La extracción de fotogramas está en [el código de video](internal/client/frames.go), y los controles de envío en [el código de estado de la cuenta](internal/client/safety.go).

## Ejemplos de uso

Con los servicios correspondientes instalados, puedes pedirle a tu asistente:

- "Busca la conversación donde acordamos el presupuesto, aunque no usaran esa palabra".
- "Resume los pendientes de este grupo y dime qué mensajes los respaldan".
- "Busca qué fecha mencionaron en las notas de voz que ya estén transcritas".
- "Muéstrame una vista previa de este video y el texto de su audio".
- "Dime cuánto historial está preparado para búsquedas y cuánto falta".

El servidor proporciona mensajes y herramientas. El asistente interpreta esa información y redacta las respuestas; la calidad depende del modelo y del historial disponible.

## Qué contenido se puede buscar

| Contenido | Cobertura |
|---|---|
| Mensajes y textos que acompañan archivos | Se incorporan a la búsqueda después de procesarlos. |
| Reacciones, contactos y ubicaciones | Se busca su representación en texto. |
| Notas de voz y audio de videos | Se busca la transcripción cuando está disponible y procesada. |
| Imágenes, stickers y fotogramas | Se busca el texto que los acompaña. No hay reconocimiento automático de texto ni búsqueda por contenido visual. |
| Documentos adjuntos | Se buscan los datos del archivo y el texto que lo acompaña. Su contenido completo no se extrae automáticamente. |

Guardar un mensaje, transcribir un audio y prepararlo para búsqueda semántica son pasos distintos. Tener el servidor funcionando no significa que todo el histórico esté procesado. `index_status` muestra la cobertura y la antigüedad de la medición.

El servidor debe mantenerse encendido para recibir eventos. Después de una desconexión puede recuperar lo que WhatsApp todavía conserve, pero no garantiza un historial completo ni la descarga de archivos vencidos.

## Instalación del servidor

Necesitas Go 1.25 o posterior para compilar y un cliente compatible con MCP por HTTP. Para usarlo en Windows, consulta [la guía de Windows](docs/windows.md). FFmpeg es necesario para extraer fotogramas y convertir audio cuando el formato lo requiere.

Desde el directorio donde quieras descargar el proyecto:

```bash
git clone https://github.com/0xjesus/whatsapp-mcp.git
cd whatsapp-mcp
make build
./bin/whatsapp-mcp serve
```

Abre <http://127.0.0.1:8765/pair>. En tu teléfono, entra a WhatsApp, **Dispositivos vinculados**, y escanea el QR. Mantén el proceso `serve` funcionando mientras uses la conexión.

Configura tu cliente MCP con esta dirección:

```text
http://127.0.0.1:8765/mcp
```

Por ejemplo, un cliente que admita configuración MCP HTTP como Claude Code puede usar:

```json
{
  "mcpServers": {
    "whatsapp": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

El formato de configuración depende del cliente. Los clientes que sólo aceptan procesos MCP por entrada y salida estándar necesitan un adaptador a HTTP.

### Activar búsqueda semántica y transcripción

El servidor base funciona sin estos componentes. Para habilitarlos:

1. Sigue [la instalación del índice de historial](docs/history-install.md). Incluye un servicio de búsqueda, PostgreSQL con pgvector y el proceso que prepara el texto para búsqueda semántica.
2. Sigue [la instalación del transcriptor](tools/transcriber/README.md). Usa Python, FFmpeg y faster-whisper para convertir voz a texto en la máquina del servidor.
3. Consulta `index_status` para revisar el avance. Las transcripciones aparecen en las búsquedas después de incorporarse al índice.

La búsqueda semántica necesita un proveedor de embeddings, las representaciones numéricas del texto que permiten comparar su significado. Puedes configurar un servicio compatible local o externo. Si usas uno externo, recibirá el texto que se procese y puede generar costos.

### Mantenerlo funcionando

Hay plantillas para [systemd en Linux](docs/systemd/whatsapp-mcp.service) y [launchd en macOS](docs/launchd/com.sealjay.whatsapp-mcp.plist). Ejecuta una sola instancia por almacén de datos.

Si el servidor está en otro equipo, accede mediante un túnel SSH y conserva la escucha local. El servidor utiliza `127.0.0.1` por defecto.

## Configuración habitual

| Variable | Uso |
|---|---|
| `WHATSAPP_MCP_ADDR` | Dirección del servidor. Por defecto, `127.0.0.1:8765`. |
| `WHATSAPP_MCP_MEDIA_ROOT` | Directorio permitido para enviar archivos y guardar descargas mediante `output_path`. |
| `WHATSAPP_MCP_FULL_SYNC` | Solicita una ventana histórica mayor al vincular la cuenta. Activado por defecto; `0` lo desactiva. |
| `WHATSAPP_MCP_HUMANIZE` | Espacia envíos, muestra que se está escribiendo y aplica límites adicionales. Activado por defecto; `0` lo desactiva. |
| `WHATSAPP_MCP_DEBUG=1` | Activa registros detallados con ocultamiento parcial de números. |
| `WHATSAPP_MCP_TOKEN` | Token exigido si habilitas conexiones remotas mediante `-allow-remote`. |

El parámetro `-store` permite elegir dónde guardar la sesión y el historial. Las rutas de archivos devueltas por las herramientas pertenecen al equipo que ejecuta el servidor.

## Herramientas más usadas

| Herramienta | Para qué sirve |
|---|---|
| `search_contacts` | Buscar contactos por nombre o número. |
| `list_chats`, `get_chat` | Consultar conversaciones. |
| `list_messages`, `get_message_context` | Buscar mensajes y recuperar su contexto. |
| `semantic_search` | Buscar por significado, texto o ambos con el servicio de historial instalado. |
| `index_status`, `history_analytics` | Consultar avance y estadísticas del índice. |
| `download_media` | Descargar archivos y entregar imágenes, audio compatible o fotogramas al asistente. |
| `request_sync` | Solicitar historial adicional de una conversación. |
| `send_message`, `send_reply`, `send_file`, `send_audio_message` | Enviar texto, respuestas y archivos. |
| `send_reaction`, `edit_message`, `delete_message` | Reaccionar, editar o eliminar mensajes según los permisos de WhatsApp. |
| `get_status`, `pairing_status` | Consultar conexión, estado de la cuenta y vinculación. |

El servidor también ofrece herramientas de grupos, encuestas, contactos y privacidad. Tu cliente puede consultar el catálogo completo mediante MCP `tools/list`.

## Privacidad y límites

- La sesión, el historial y las descargas se guardan en el equipo del servidor. Los datos de sesión permiten acceder a la cuenta: mantenlos privados y fuera del repositorio.
- El asistente puede enviar al proveedor de su modelo el contenido recuperado por una herramienta. La transcripción local no convierte toda la integración en un sistema sin servicios externos.
- Los mensajes recibidos son contenido de terceros. El asistente debe tratarlos como información, sin ejecutar instrucciones que aparezcan dentro de ellos.
- Este proyecto es independiente y usa un cliente no oficial. No está afiliado a Meta ni a WhatsApp. Los límites de envío no eliminan el riesgo de restricciones de cuenta.
- Las transcripciones dependen de la disponibilidad del archivo y de los límites del transcriptor. La muestra de fotogramas no equivale a revisar cada instante de un video.
- El ocultamiento parcial en registros no equivale a anonimizar los datos.

## Desarrollo y problemas comunes

```bash
make test
make test-race
make lint
make e2e
```

Los trabajadores de historial y transcripción tienen sus propias pruebas, descritas en sus guías. Consulta [cómo contribuir](CONTRIBUTING.md) antes de enviar cambios.

| Problema | Qué revisar |
|---|---|
| No conecta o no está vinculado | Abre la página de vinculación y consulta `get_status`. |
| Ya existe una instancia usando los datos | Mantén un único proceso `serve` por almacén. |
| La cuenta aparece como restringida | Detén los envíos. Evita reinicios o nuevas vinculaciones repetidas. |
| La búsqueda semántica no está disponible | Revisa que el servicio de historial esté instalado y funcionando. |
| Un video se descarga sin fotogramas | Comprueba FFmpeg y el campo `FramesError` de la respuesta. |
| Faltan mensajes o transcripciones | Revisa cobertura, disponibilidad de archivos y límites de procesamiento. |

## Licencia

[MIT](LICENSE). Se conservan los créditos de [Sealjay/mcp-whatsapp](https://github.com/Sealjay/mcp-whatsapp) y [lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp).
