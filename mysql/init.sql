CREATE TABLE IF NOT EXISTS Clientes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255),
    email VARCHAR(255),
    telefone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Produtos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    categoria VARCHAR(100),
    preco DECIMAL(10,2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS Vendas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    produto_id INT,
    quantidade INT NOT NULL,
    data_venda DATE,
    FOREIGN KEY (produto_id) REFERENCES Produtos(id)
);

CREATE TABLE IF NOT EXISTS Satisfacao (
    id INT AUTO_INCREMENT PRIMARY KEY,
    produto_id INT,
    nota INT CHECK (nota BETWEEN 1 AND 5),
    comentario TEXT,
    data_avaliacao DATE,
    FOREIGN KEY (produto_id) REFERENCES Produtos(id)
);

