import org.apache.nifi.processor.io.*
import org.apache.nifi.processor.*
import java.nio.charset.StandardCharsets

def flowFile = session.create()
if (!flowFile) return

def urls = [
    "https://data.electricitymaps.com/2025-04-03/IT_2021_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/IT_2022_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/IT_2023_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/IT_2024_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/SE_2021_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/SE_2022_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/SE_2023_hourly.csv",
    "https://data.electricitymaps.com/2025-04-03/SE_2024_hourly.csv"
]

urls.each { url ->
    def ff = session.create()
    ff = session.write(ff, { outputStream ->
        outputStream.write(url.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    ff = session.putAttribute(ff, "filename", url.tokenize("/")[-1])
    ff = session.putAttribute(ff, "download_url", url)
    session.transfer(ff, REL_SUCCESS)
}

session.remove(flowFile)

