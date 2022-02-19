import logging
import http.client
import urllib.parse
import json
import boto3
from datetime import datetime
from botocore.config import Config

_logger = logging.getLogger()
_logger.setLevel(logging.INFO)

SLACK_WEBHOOK_URL = "<url-hook>"
SLACK_CANAL_DE_NOTIFICACAO = "#emr-cluster"  # prefixo @ para usuarios, # para grupos

class EMRClusterTerminate(object):
    logger = _logger

    def __init__(self):
        self.emr_client = None
        self.emr_clusters_ativos = None

    def run(self):
        self._set_emr_client()
        self._listar_clusters_ativos()
        self._log_numero_de_clusters_Ativos()
        self._slack_envia_notificacao_para_cada_cluster_ativo()
        self._listar_clusters_em_string()
        self._listar_steps_emr()
        self._gravar_steps_s3()
        self._emr_terminate_clusters_ativos()

    def _set_emr_client(self):
        configuracao_emr = Config(
            region_name = 'sa-east-1',
            retries = {
                'max_attempts': 10,
                'mode': 'standard'
            }
        )
        session = boto3.Session()
        self.emr_client = session.client('emr', config=configuracao_emr)

    def _listar_clusters_ativos(self):
        emr_estado_do_cluster_ativo = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        response = self.emr_client.list_clusters(ClusterStates=emr_estado_do_cluster_ativo)
        self.emr_clusters_ativos = [cluster["Id"] for cluster in response["Clusters"]]

    def _log_numero_de_clusters_Ativos(self):
        if not self.emr_clusters_ativos:
            self.logger.info("Sem clusters EMR ativos no momento...")
        else:
            self.logger.info("Encontrados {} clusters ativos...".format(len(self.emr_clusters_ativos)))

    def _slack_envia_notificacao_para_cada_cluster_ativo(self):
        for emr_cluster_id in self.emr_clusters_ativos:
            self._slack_notificacao_para_cluster_ativo(emr_cluster_id)

    def _slack_notificacao_para_cluster_ativo(self, emr_cluster_id):
        descricao = self._emr_descricao_do_cluster(emr_cluster_id)
        mensagem = self._slack_coleta_mensagem_da_descricao(descricao)
        icone = self._icone_emoji_baseado_na_descricao(descricao)
        usuario = self._coleta_usuario(descricao)
        self._slack_envia_notificacao(mensagem, icone, usuario)

    def _emr_descricao_do_cluster(self, emr_cluster_id):
        descricao = self.emr_client.describe_cluster(ClusterId=emr_cluster_id)
        state = descricao['Cluster']['Status']['State']
        name = descricao['Cluster']['Name']
        keypair = descricao['Cluster']['Ec2InstanceAttributes']['Ec2KeyName']
        descricao = {'state': state, 'name': name, 'keypair': keypair}
        return descricao

    def _slack_coleta_mensagem_da_descricao(self, descricao):
        mensagem = "Cluster `{name}` ainda está ativo com o estado `{state}` e a keypair `{keypair}`. Efetuando o terminate do Cluster EMR  `{name}`." \
                  .format(state=descricao['state'], name=descricao['name'], keypair=descricao['keypair'])
        self.logger.info("mensagem: {}".format(mensagem))
        return mensagem

    def _icone_emoji_baseado_na_descricao(self, descricao):
        keypair = self._coleta_a_keypair(descricao)
        if keypair == "thom":
            return ":thom:"
        else:
            return ":money_with_wings:"

    def _coleta_usuario(self, descricao):
        keypair = self._coleta_a_keypair(descricao)
        usuario = "EMR Cluster Bot ({})".format(keypair)
        return usuario

    @staticmethod
    def _coleta_a_keypair(descricao):
        return descricao["keypair"]

    @staticmethod
    def _slack_envia_notificacao(mensagem, icone, usuario):
        slack_notifier = SlackNotificador()
        slack_notifier.slack_envia_mensagem(mensagem, icone, usuario)

    def _listar_clusters_em_string(self):
        clusters = self.emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        clusters_resultado = clusters['Clusters']
        cluster_ids = clusters_resultado[0]["Id"]
        return cluster_ids

    def _listar_steps_emr(self):
        id_para_steps = self._listar_clusters_em_string()
        emr_listagem_de_steps = self.emr_client.list_steps(
            ClusterId = id_para_steps,
            StepStates=[
                'PENDING', 'CANCEL_PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED',
            ],
        )
        return emr_listagem_de_steps

    def _gravar_steps_s3(self):
        steps_para_gravar = self._listar_steps_emr()
        s3 = boto3.resource('s3')
        object = s3.Object('fernandomullerjr', 'bkp-emr-steps/backup-steps-emr.json')
        json_data = json.dumps(steps_para_gravar, indent=2, default=str)
        result = object.put(Body=json_data)
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:
            print('Arquivo com os steps foi enviado com sucesso ao bucket fernandomullerjr!')
        else:
            print('Arquivo não foi enviado.')

    def _emr_terminate_clusters_ativos(self):
        self.emr_client.terminate_job_flows(
            JobFlowIds=self.emr_clusters_ativos
        )
        self.logger.info("Efetuado terminate de todos os clusters EMR ativos...")


class SlackNotificador(object):
    logger = _logger

    def __init__(self):
        self.canal = SLACK_CANAL_DE_NOTIFICACAO
        self.slack_webhook_url = SLACK_WEBHOOK_URL

    def slack_envia_mensagem(self, mensagem, icone, usuario):
        payload = self._coleta_payload(usuario, icone, mensagem)
        dados = self. _coleta_os_dados_codificados_dos_objetos(payload)
        headers = self._coleta_os_headers()
        response = self._envia_requisicao_post(dados, headers)
        self._log_status_de_resposta_slack(response)

    def _coleta_payload(self, usuario, icone, mensagem):
        payload_dict = {
            'canal': self.canal,
            'usuario': usuario,
            'icon_emoji': icone,
            'text': mensagem,
            "attachments": [
                {
                    "color": "#36a64f",
                    "title": "Stop/Start - Cluster EMR.",
                    "image_url": "https://symbols.getvecta.com/stencil_5/3_aws-emr.115b439538.png",
                    "thumb_url": "https://symbols.getvecta.com/stencil_5/3_aws-emr.115b439538.png",
                }
            ]
        }
        payload = json.dumps(payload_dict)
        return payload

    @staticmethod
    def _coleta_os_dados_codificados_dos_objetos(payload):
        values = {'payload': payload}
        str_values = {}
        for k, v in values.items():
            str_values[k] = v.encode('utf-8')
        dados = urllib.parse.urlencode(str_values)
        return dados

    @staticmethod
    def _coleta_os_headers():
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        return headers

    def _envia_requisicao_post(self, body, headers):
        https_connection = self._coleta_conexoes_https_com_o_slack()
        https_connection.request('POST', self.slack_webhook_url, body, headers)
        response = https_connection.getresponse()
        return response

    @staticmethod
    def _coleta_conexoes_https_com_o_slack():
        h = http.client.HTTPSConnection('hooks.slack.com')
        return h

    def _log_status_de_resposta_slack(self, response):
        if response.status == 200:
            self.logger.info("Enviada mensagem ao Slack com sucesso.")
        else:
            self.logger.critical("Envio de mensagem ao Slack falhou com o "
                                 "código de status '{}' e a causa '{}'.".format(response.status, response.reason))

def lambda_handler(event, context):
    EMRClusterTerminate().run()

print('Iniciando o terminate...')
EMRClusterTerminate().run()
print('Concluindo o terminate.')

