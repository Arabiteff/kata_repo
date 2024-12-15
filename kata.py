from rework.pipeline import FlightPipeline
import os
if __name__ == "__main__":
    pipeline = FlightPipeline()
    pipeline.extract()
    results=pipeline.transform()
    pipeline.loading(results)
