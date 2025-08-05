FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependências para o container
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação para dentro do container
COPY . .

# Expõe a porta padrão do Uvicorn
EXPOSE 8000

# Comando para iniciar a aplicação
#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# Comando para iniciar a aplicação e atualizar conforme mudança no codigo
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

