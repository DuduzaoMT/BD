#!/usr/bin/python3
# Copyright (c) BDist Development Team
# Distributed under the terms of the Modified BSD License.
import os
from logging.config import dictConfig

from flask import Flask, jsonify, request
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

# Use the DATABASE_URL environment variable if it exists, otherwise use the default.
# Use the format postgres://username:password@hostname/database_name to connect to the database.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://saude:saude@postgres/saude")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": True,  # If True don’t start transactions automatically.
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger


@app.route("/", methods=("GET",))
def clinica():
    """Mostra todas as clinicas disponives."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            clinicas = cur.execute(
                """
                SELECT nome, morada
                FROM clinica
                ORDER BY nome DESC;
                """,
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")
            if cur.rowcount == 0:
                return jsonify({"message": "Não foram encontradas clinicas.", "status": "error"}), 40
            
    new = [[clinica[0], clinica[1]] for clinica in clinicas]
    return jsonify(new), 200

def verifica_clinica(cur,clinica):
    cur.execute(
        """
        SELECT nome
        FROM clinica c
        WHERE c.nome = %(clinica)s; 
        """,
        {"clinica": clinica},
    )
    log.debug(f"Found {cur.rowcount} rows.")

    if cur.rowcount == 0:
        return False
    else:
        return True

@app.route("/c/<clinica>/", methods=("GET",))
def clinica_especialidade(clinica):
    """Mostra todas as especialidades da clinica especificada."""

    with pool.connection() as conn:
        with conn.cursor() as cur:

            if not verifica_clinica(cur, clinica):
                return jsonify({"message": "Clínica não encontrada.", "status": "error"}), 404
            
            table = cur.execute(
                """
               SELECT DISTINCT especialidade
               FROM trabalha t JOIN medico m using (nif)
               WHERE t.nome = %(clinica)s;
                """,
                {"clinica": clinica},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

            if cur.rowcount == 0:
                return jsonify({"message": "Não foram encontradas especialidades.", "status": "error"}), 404
        
    especialidades = [row[0] for row in table]  # Extrair valores da coluna 'especialidade'
    return jsonify(especialidades), 200


def date_parser(date):
    d = str(date)
    d = d.split('(')
    return d[0]

def verifica_especialidade(cur, especialidade):
    cur.execute(
        """
        SELECT DISTINCT especialidade
        FROM medico m 
        WHERE m.especialidade = %(especialidade)s; 
        """,
        {"especialidade": especialidade},
    )
    log.debug(f"Found {cur.rowcount} rows.")

    if cur.rowcount == 0:
        return False
    else:
        return True

@app.route("/c/<clinica>/<especialidade>/", methods=("GET",))
def horarios_disponiveis(clinica, especialidade):
    """Lista todos os médicos (nome) da especialidade e clínica especificadas
    e os primeiros três horários disponíveis para consulta de cada um deles (data e hora).
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                if not verifica_clinica(cur, clinica):
                    raise Exception("Clínica não encontrada.")

                if not verifica_especialidade(cur, especialidade):
                    raise Exception("Especialidade não encontrada.")
                
                medicos_especialidade = cur.execute(
                    """
                    WITH horarios_totais_medico_clinica AS (SELECT *
                        FROM combinacoes_temp_med
                        WHERE especialidade = %(especialidade)s and nome_clinica = %(clinica)s and 
                        (
                            DATE(NOW()) < data OR
                            (
                            DATE(NOW()) = data AND EXTRACT(HOUR FROM hora)> EXTRACT(HOUR FROM NOW())+1
                            ) OR
                            (
                            DATE(NOW()) = data AND EXTRACT(HOUR FROM hora) = EXTRACT(HOUR FROM NOW())+1 AND EXTRACT(MINUTE FROM hora) > EXTRACT(MINUTE FROM NOW())
                            )
                        )
                    )

                    SELECT nome_medico,data,hora 
                    FROM horarios_totais_medico_clinica LEFT JOIN consulta using (nif, data, hora)
                    WHERE consulta.id IS NULL
                    ORDER BY data,hora;

                    """,
                    {"especialidade": especialidade, "clinica": clinica},
                ).fetchall()

            except Exception as e:
                return jsonify({"message": str(e), "status": "error"}), 404
            
            else:
                log.debug(f"Found {cur.rowcount} rows.")

                if cur.rowcount == 0:
                    return (
                        jsonify({"message": "Não foram encontrados médicos.", "status": "error"}),404
                    )


    medicos = {}
    for row in medicos_especialidade:
        if row[0].strip() not in medicos:
            medicos[row[0].strip()] = [ [date_parser(row[1]), date_parser(row[2])]]
        else:
            if len( medicos[row[0].strip()]) < 3:
                medicos[row[0].strip()].append( [date_parser(row[1]), date_parser(row[2])])

    return jsonify(medicos), 200

def verifica_ssn(cur,ssn):
    cur.execute(
        """
        SELECT *
        FROM paciente
        WHERE ssn = %(ssn)s;
        """,
        {"ssn": ssn},
    )
    log.debug(f"Found {cur.rowcount} rows.")

    if cur.rowcount == 0:
        return False
    else:
        return True
    
def verifica_nif(cur,nif):
    cur.execute(
        """
        SELECT *
        FROM medico
        WHERE nif = %(nif)s;
        """,
        {"nif": nif},
    )
    log.debug(f"Found {cur.rowcount} rows.")
    if cur.rowcount == 0:
        return False
    else:
        return True
            
def data_hora_parse(data,hora):

    data = str(data)
    hora = str(hora)
    
    # Verifica se a string tem o comprimento correto e se os separadores estão nas posições corretas
    if len(data) != 10 or data[4] != '-' or data[7] != '-':
        return False
    
    # Verifica se os componentes da data são dígitos
    if not (data[:4].isdigit() and data[5:7].isdigit() and data[8:].isdigit()):
        return False
    
    # Verifica o formato da hora
    if len(hora) != 5:
        return False
    
    if len(hora) == 5:
        if hora[2] != ':' or (not hora[:2].isdigit()) or (not hora[3:5].isdigit()):
            return False

    return True

def verifica_data_atual(cur,data,hora):
    bool = cur.execute(
        """
        SELECT DATE(NOW()) < %(data)s::date OR
        (
        DATE(NOW()) = %(data)s::date AND EXTRACT(HOUR FROM %(hora)s::time)> EXTRACT(HOUR FROM NOW())+1
        ) OR
        (
        DATE(NOW()) = %(data)s::date AND EXTRACT(HOUR FROM %(hora)s::time) = EXTRACT(HOUR FROM NOW())+1 AND EXTRACT(MINUTE FROM %(hora)s::time) > EXTRACT(MINUTE FROM NOW())
        )
        """,
        {"data": data, "hora": hora}
    ).fetchone()
    log.debug(f"Found {bool[0]} rows.")
    if str(bool[0]) == 'True':
        return True
    else:
        return False
        
def verifica_data_hora(cur,nif,data,hora):
    # verificar se o medico tem disponibilidade
    cur.execute(
        """
        SELECT *
        FROM consulta
        WHERE nif = %(nif)s and  data = %(data)s and hora = %(hora)s;
        """,
        {"nif": nif,"data":data,"hora":hora},
    )
    log.debug(f"Found {cur.rowcount} rows.")
    if cur.rowcount == 0:
        return True
    else:
        return False
                
def verifica_data_hora_ssn(cur,ssn,data,hora):
    # verificar se o paciente tem disponibilidade
    cur.execute(
        """
        SELECT *
        FROM consulta
        WHERE ssn = %(ssn)s and  data = %(data)s and hora = %(hora)s;
        """,
        {"ssn": ssn,"data":data,"hora":hora},
    )
    log.debug(f"Updated {cur.rowcount} rows.")
    if cur.rowcount == 0:
        return True
    else:
        return False


@app.route(
    "/a/<clinica>/registar/",
    methods=(
        "PUT",
        "POST",
    ),
)
def registar(clinica):
    """Registra uma marcação de consulta na <clinica> na base
    de dados (populando a respectiva tabela). Recebe como
    argumentos um paciente, um médico, e uma data e hora
    (posteriores ao momento de agendamento).
    """

    ssn = request.args.get("ssn")
    nif = request.args.get("nif")
    data = request.args.get("data")
    hora = request.args.get("hora")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                with conn.transaction():
                    if not ssn or not nif or not data or not hora:
                        raise Exception("Introduza os argumentos.")
                    if not verifica_clinica(cur, clinica):
                        raise Exception("Clínica não encontrada.")
                    if not verifica_ssn(cur,ssn):
                        raise Exception("Introduza um SSN válido.")
                    if not verifica_nif(cur,nif):
                        raise Exception("Introduza um NIF válido.")
                    if not data_hora_parse(data,hora):
                        raise Exception("Introduza uma data (AAAA-MM-DD) e horario (HH:MM) (08:00 as 13:00 e das 14:00 as 20:00 em intervalos de 30min) válidos")
                    if not verifica_data_atual(cur,data,hora):
                        raise Exception("Só são permitidos registos de consultas posteriores à data e hora atuais.")
                    if not verifica_data_hora(cur,nif,data,hora):
                        raise Exception(f"O medico {nif} não está disponivel neste horario.")
                    if not verifica_data_hora_ssn(cur,ssn,data,hora):
                        raise Exception(f"O paciente {ssn} não está disponivel neste horario.")
                    cur.execute(
                        """
                        INSERT INTO consulta (ssn,nif,nome,data,hora)
                        VALUES (%(ssn)s,%(nif)s,%(clinica)s,%(data)s,%(hora)s)
                        """,
                        {"clinica": clinica, "ssn": ssn,"nif": nif, "data": data, "hora": hora},
                    )

            # Para tratar as exceções lançadas em cima e a exceção do medico nao trabalhar na clinica naquele dia da semana e 
            # não se poder consultar a ele próprio.
            except Exception as e:
                if ('violates check constraint' in str(e) or 'date/time field value out of range' in str(e)):
                    return jsonify({"message": "Introduza uma data (AAAA-MM-DD) e horario (HH:MM) (08:00 as 13:00 e das 14:00 as 20:00 em intervalos de 30min) válidos", "status": "error"}), 400

                return jsonify({"message": str(e).split('.')[0], "status": "error"}), 400
            else:
                log.debug(f"Updated {cur.rowcount} rows.")

    return "", 204

def verifica_consulta(cur,clinica,ssn,nif,data,hora):
    # verificar se a consulta existe
    cur.execute(
        """
        SELECT id
        FROM consulta
        WHERE ssn = %(ssn)s and nif = %(nif)s and nome = %(clinica)s and data = %(data)s and hora = %(hora)s
        """,
        {"clinica": clinica, "ssn": ssn,"nif": nif, "data": data, "hora": hora},
    )
    
    if cur.rowcount == 0:
        return False
    else:
        return True

@app.route(
    "/a/<clinica>/cancelar/",
    methods=(
        "DELETE",
        "POST",
    ),
)
def cancelar_consulta(clinica):
    """Cancela uma marcação de consulta que ainda não se realizou na <clinica> 
    (o seu horário é posterior ao momento do cancelamento), 
    removendo a entrada da respectiva tabela na base de dados. 
    Recebe como argumentos um paciente, um médico, e uma data e hora."""

    ssn = request.args.get("ssn")
    nif = request.args.get("nif")
    data = request.args.get("data")
    hora = request.args.get("hora")
    
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                with conn.transaction():
                    if not ssn or not nif or not data or not hora:
                        raise Exception("Introduza os argumentos.")
                    if not verifica_ssn(cur,ssn):
                        raise Exception("Introduza um SSN válido.")
                    if not verifica_nif(cur,nif):
                        raise Exception("Introduza um NIF válido.")
                    if not data_hora_parse(data,hora):
                        raise Exception("Introduza uma data (AAAA-MM-DD) e horario (HH:MM) (08:00 as 13:00 e das 14:00 as 20:00 em intervalos de 30min) válidos")
                    if not verifica_data_atual(cur,data,hora):
                        raise Exception("Só são permitidos cancelamentos de consultas posteriores à data e hora atuais.")
                    if not verifica_consulta(cur,clinica,ssn,nif,data,hora):
                        raise Exception("Marcação não encontrada.")
                    cur.execute(
                        """
                        DELETE FROM consulta
                        WHERE id IN (
                            SELECT id
                            FROM consulta
                            WHERE ssn = %(ssn)s and nif = %(nif)s and nome = %(clinica)s and data = %(data)s and hora = %(hora)s 
                        )
                        """,
                        {"clinica": clinica, "ssn": ssn,"nif": nif, "data": data, "hora": hora},
                    )

            # Para tratar as exceções lançadas
            except Exception as e:
                if ('violates check constraint' in str(e)):
                    return jsonify({"message": "Introduza uma data (AAAA-MM-DD) e horario (HH:MM) (08:00 as 13:00 e das 14:00 as 20:00 em intervalos de 30min) válidos", "status": "error"}), 400
                return jsonify({"message": str(e), "status": "error"}), 400
            
            else:
                log.debug(f"Deleted {cur.rowcount} rows.")

    return "", 204


if __name__ == "__main__":
    app.run()
