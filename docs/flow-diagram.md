# Data flow diagram

The diagrams below show the flow of data through the system. The main purpose is
highlight the distinction between the data preparation pipeline (`poppusher`)
and the data access components (`popgetter-*`).

```mermaid
---
title: Poppusher
---
graph LR
    subgraph download [Download country data]
        census_download@{ shape: text, label: "(customised for each country's census format)" }
        aA(Scotland)
        aB(Northern Ireland)
        aC(Singapore)
        aD(USA)
        aE(Belgium)
        aF(Australia)
    end

    raw(raw data)

    aA & aB & aC & aD & aE & aF --> raw ==> ingest

    subgraph ingest
        bA(Convert to common file formats)
        bB(Derive common metadata info)
        bC(Derive common metrics)
        bA --> bB
        bB --> bA
        bC --> bB
        bB --> bC
    end

    direction TB
    subgraph processed [Cloud hosted structure data store]
        direction TB
        dir_struct_docs@{ shape: text, label: "(**_See docs_**)" }
        dA("`**countries**
          (plain-text)`")
        subgraph percountry [per-country files]
            dCa("`**metadata**
            (parquet)`")
            dCb("`**metrics**
            (parquet)`")
            dCc("`**geometry**
              - (flatgeobuff)
              - (GeoJSON)
              - (PMtiles)`")
        end
        dir_struct_docs ~~~ dA
        dA ~~~ percountry
        click dir_struct_docs href "https://poppusher.readthedocs.io/en/latest/output_structure/" _blank
    end

    ingest ==> processed

```

```mermaid
---
title: Popgetter
---
graph LR
    subgraph processed [Cloud hosted structure data store]
        direction TB
        dir_struct_docs@{ shape: text, label: "(**_See docs_**)" }
        dA("`**countries**
          (plain-text)`")
        subgraph percountry [per-country files]
            dCa("`**metadata**
            (parquet)`")
            dCb("`**metrics**
            (parquet)`")
            dCc("`**geometry**
              - (flatgeobuff)
              - (GeoJSON)
              - (PMtiles)`")
        end
        dir_struct_docs ~~~ dA
        dA ~~~ percountry
        click dir_struct_docs href "https://poppusher.readthedocs.io/en/latest/output_structure/" _blank
    end

    direction TB
    subgraph clients
        core("`**popgetter-core**
        common part of all clients
        - complied to wasm.
        - understands the directory structure
        and downloads the data.
        `")
        direction TB
        fA("`**popgetter-cli**
          A commandline tools to query and download data`")
        fB("`**popgetter-py**
          Enables access from Python`")
        fC("`**popgetter-browser**
          A web interface for exploring the available data`")
        fD("`**popgetter-llm**
          An experimental natural language client using LLMs`")
        core --> fA
        core --> fB
        core --> fC
        core --> fD
    end

    processed ===> core
```
