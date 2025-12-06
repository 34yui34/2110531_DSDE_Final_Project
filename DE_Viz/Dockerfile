ARG TARGETARCH

FROM 34yui34/dsde-final-proj-container:latest-${TARGETARCH}

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /DE_Viz

COPY . .

RUN python --version && java -version

EXPOSE 8501

CMD ["streamlit", "run", "map_visualization.py", "--server.address", "0.0.0.0"]
