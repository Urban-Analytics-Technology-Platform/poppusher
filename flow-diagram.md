# Draft flow diagram

This is a concept diagram for the flow of data through the system. It is not
complete nor authoritative. It is intended to be a starting point for
discussion.

TODO:

- Add details about the ingest process
- Highlight components/functionality that we are likely to need to implement
  ourselves.
- Highlight components/functionality that which could be adopted from a
  third-party pipeline management tool (e.g. Airflow, Dagster, etc).
- Improve layout and styling (e.g. the position of the cache box is not ideal).

```mermaid
flowchart LR
    subgraph download [Download country data]
        aA(Scotland)
        aB(Northern Ireland)
        aC(Singapore)
        aD(USA)
        aE(Belgium)
        aF(Australia)
    end

    raw(raw data)
    processed(processed data)

    aA & aB & aC & aD & aE & aF --> raw ==> ingest

    subgraph cache
        direction LR
        dA[(file store)]
        dB[(structure data store)]
        dA ~~~ dB
    end

    dA <-.-> raw
    dB <-.-> processed
    download ~~~ cache
    raw ~~~ cache
    cache ~~~ processed

    subgraph ingest
        bA(Convert to common technical format)
        bB(Interpolate to common semantics)
        bA --> bB
        bB --> bA
    end


    subgraph downstream [Downstream Applications]
        direction TB
        cA(SPENSER)
        cB(LTN tool)
        cC(etc)
        cA ~~~ cB ~~~ cC
    end

    ingest ==> processed ==> downstream

```
