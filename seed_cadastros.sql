-- =============================================
-- BANCOS DA CKM
-- =============================================
INSERT INTO bancos (codigo, nome, agencia, conta, ativo) VALUES
  ('BB',  'Banco do Brasil',  NULL, NULL, TRUE),
  ('ITAU','Itaú Unibanco',    NULL, NULL, TRUE),
  ('STD', 'Santander',        NULL, NULL, TRUE)
ON CONFLICT (codigo) DO NOTHING;

-- =============================================
-- TIPOS DE LANÇAMENTO
-- =============================================
INSERT INTO tipos_lancamento (codigo, nome, natureza) VALUES
  ('NF_SERVICO',    'Nota Fiscal de Serviço',      'RECEITA'),
  ('ADIANTAMENTO',  'Adiantamento de Cliente',     'RECEITA'),
  ('DEVOLUCAO',     'Devolução / Reembolso',        'RECEITA'),
  ('MUTUO_ENTRADA', 'Mútuo Recebido',               'RECEITA'),
  ('FOLHA',         'Folha de Pagamento / Salário', 'DESPESA'),
  ('FORNECEDOR',    'Pagamento a Fornecedor',       'DESPESA'),
  ('ALUGUEL',       'Aluguel / Escritório',         'DESPESA'),
  ('IMPOSTO',       'Imposto / Tributo',            'IMPOSTO'),
  ('SIMPLES',       'Simples Nacional',             'IMPOSTO'),
  ('INSS',          'INSS',                         'IMPOSTO'),
  ('PRONAMPE',      'Parcela Pronampe',             'DESPESA'),
  ('MUTUO_SAIDA',   'Mútuo Pago / Devolvido',       'DESPESA'),
  ('TARIFA_BANCO',  'Tarifa Bancária',              'FINANCEIRO'),
  ('IOF',           'IOF',                          'FINANCEIRO'),
  ('TEF',           'Transferência entre Contas',   'TRANSFERENCIA'),
  ('OUTROS',        'Outros',                       'DESPESA')
ON CONFLICT (codigo) DO NOTHING;

-- =============================================
-- CENTROS DE CUSTO
-- =============================================
INSERT INTO centros_de_custo (codigo, nome, tipo) VALUES
  ('ESCRITORIO',   'Escritório / Infraestrutura',    'ESTRUTURA'),
  ('SALARIOS',     'Salários e Encargos',            'ESTRUTURA'),
  ('JURIDICO',     'Jurídico',                       'ESTRUTURA'),
  ('CONTABIL',     'Contabilidade',                  'ESTRUTURA'),
  ('MARKETING',    'Marketing e Comunicação',        'ESTRUTURA'),
  ('TI',           'Tecnologia da Informação',       'ESTRUTURA'),
  ('ADM',          'Administração Geral',            'ESTRUTURA'),
  ('PRONAMPE',     'Pronampe (Empréstimo BB)',        'FINANCEIRO'),
  ('MUTUO',        'Mútuo (Empréstimos Sócios)',      'FINANCEIRO'),
  ('BANCO',        'Tarifas e Despesas Bancárias',   'FINANCEIRO'),
  ('IOF_CC',       'IOF',                            'FINANCEIRO'),
  ('SEBRAE_TO',    'SEBRAE-TO',                      'OPERACIONAL'),
  ('SEBRAE_AC',    'SEBRAE-AC',                      'OPERACIONAL'),
  ('BRB',          'BRB',                            'OPERACIONAL'),
  ('BANRISUL',     'BANRISUL',                       'OPERACIONAL'),
  ('CESAMA',       'CESAMA',                         'OPERACIONAL'),
  ('MARICA',       'P.M. Maricá',                    'OPERACIONAL'),
  ('IGDRH',        'IGD-RH / IGDRH',                 'OPERACIONAL'),
  ('ENABLE',       'Enable People',                  'OPERACIONAL'),
  ('TEF_CC',       'Transferência entre Contas',     'TRANSFERENCIA')
ON CONFLICT (codigo) DO NOTHING;

-- =============================================
-- CLIENTES
-- =============================================
INSERT INTO clientes (codigo, nome, nome_curto, ativo) VALUES
  ('SEBRAE_TO',  'SEBRAE Tocantins',                    'SEBRAE-TO',  TRUE),
  ('SEBRAE_AC',  'SEBRAE Acre',                         'SEBRAE-AC',  TRUE),
  ('BRB',        'BRB - Banco de Brasília',             'BRB',        TRUE),
  ('BANRISUL',   'Banrisul',                            'BANRISUL',   TRUE),
  ('CESAMA',     'CESAMA',                              'CESAMA',     TRUE),
  ('MARICA',     'Prefeitura Municipal de Maricá',      'P.M.MARICÁ', TRUE),
  ('IGDRH',      'IGD-RH',                              'IGD-RH',     TRUE),
  ('ENABLE',     'Enable People',                       'ENABLE',     TRUE),
  ('FIOCRUZ',    'Fiocruz',                             'FIOCRUZ',    FALSE),
  ('BRB_CARD',   'BRB Card',                            'BRB CARD',   TRUE)
ON CONFLICT (codigo) DO NOTHING;

-- =============================================
-- PROJETOS
-- =============================================
INSERT INTO projetos (codigo, nome, tipo) VALUES
  ('4.1',  'Treinamentos / Assessment / Cursos',  'CONSULTORIA'),
  ('4.2',  'Palestra',                             'CONSULTORIA'),
  ('4.3',  'Coaching',                             'CONSULTORIA'),
  ('4.4',  'PDI do BEM',                           'PROGRAMA'),
  ('4.5',  'Liderança',                            'CONSULTORIA'),
  ('4.6',  'PDI Evoluir',                          'PROGRAMA'),
  ('4.7',  'Mentoria',                             'CONSULTORIA'),
  ('4.8',  'Processo Seletivo',                    'RECRUTAMENTO'),
  ('4.9',  'Pesquisa de Clima',                    'CONSULTORIA'),
  ('4.10', 'Avaliação de Desempenho',              'CONSULTORIA'),
  ('4.11', 'Estágio Probatório',                   'PROGRAMA'),
  ('4.12', 'Onboarding',                           'CONSULTORIA'),
  ('4.13', 'Cultura Organizacional',               'CONSULTORIA'),
  ('4.14', 'Gestão de Talentos',                   'CONSULTORIA'),
  ('4.15', 'Outros Projetos',                      'OUTROS')
ON CONFLICT (codigo) DO NOTHING;

SELECT 
  (SELECT COUNT(*) FROM bancos) AS bancos,
  (SELECT COUNT(*) FROM tipos_lancamento) AS tipos,
  (SELECT COUNT(*) FROM centros_de_custo) AS centros_custo,
  (SELECT COUNT(*) FROM clientes) AS clientes,
  (SELECT COUNT(*) FROM projetos) AS projetos;
