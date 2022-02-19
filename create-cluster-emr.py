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

class EMRClusterCreate(object):
    logger = _logger

    def __init__(self):
        self.emr_client = None

    def run(self):
        self._set_emr_client()
        self._emr_create_cluster()
        self._slack_notificacao()

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

    def _emr_create_cluster(self):
        emr_cluster_job_creation = self.emr_client.run_job_flow(
            Name='cluster_emr_teste',
            LogUri='s3://fernandomullerjr/bkp-emr-steps',
            ReleaseLabel='emr-6.4.0',
            Applications=[
                {
                    'Name': 'Spark'
                },
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': 'devops-fernando-SP-20-10-2021',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-049de287468677d35',
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'tag_name_1',
                    'Value': 'tab_value_1',
                },
                {
                    'Key': 'tag_name_2',
                    'Value': 'tag_value_2',
                },
            ],
            BootstrapActions=[
                {
                    'Name': 'install-boto3',
                    'ScriptBootstrapAction': {
                    'Path': 's3://fernandomullerjr/bootstrap-actions/install-boto3.sh',
                    'Args': [
                    'string',
                ]
                }
                },
            ],
        )
        print ('Criado o Cluster EMR...', emr_cluster_job_creation['JobFlowId'])
        self.emr_cluster_id = emr_cluster_job_creation['JobFlowId']

    def _slack_notificacao(self, emr_cluster_id):
        descricao = self._emr_descricao_do_cluster(emr_cluster_id=emr_cluster_id)
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
        mensagem = "Criando o Cluster EMR `{name}`, com a keypair `{keypair}` . O estado atual do Cluster é: `{state}`." \
                  .format(state=descricao['state'], name=descricao['name'], keypair=descricao['keypair'])
        self.logger.info("mensagem: {}".format(mensagem))
        return mensagem

    def _icone_emoji_baseado_na_descricao(self, descricao):
        keypair = self._coleta_a_keypair(descricao)
        if keypair == "thom":
            return ":warning:"
        else:
            return ":warning:"

    @staticmethod
    def _coleta_a_keypair(descricao):
        return descricao["keypair"]

    def _coleta_usuario(self, descricao):
        keypair = self._coleta_a_keypair(descricao)
        usuario = "EMR Cluster Bot ({})".format(keypair)
        return usuario

    @staticmethod
    def _slack_envia_notificacao(mensagem, icone, usuario):
        slack_notifier = SlackNotificador()
        slack_notifier.slack_envia_mensagem(mensagem, icone, usuario)

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
    EMRClusterCreate().run()

print('Iniciando a criação do Cluster EMR...')
EMRClusterCreate().run()
print('Concluindo a criação do Cluster EMR.')
