/**
 * seed_grupos_tipos.js
 * Popula as tabelas grupos_despesa e tipos_despesa com a estrutura gerencial da CKM.
 * Executar: node seed_grupos_tipos.js
 */
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : false,
  connectionTimeoutMillis: 10000,
});

const ESTRUTURA = [
  ['PESSOAL', 'Pessoal', [
    ['PROLABORE','Pró-labore'],['SALARIOS','Salários'],['PJ_INTERNOS','Prestadores PJ Internos'],
    ['BENEFICIOS','Benefícios'],['ASSIST_MEDICA','Assistência Médica'],['VALE_TRANSPORTE','Vale Transporte'],
    ['VALE_REFEICAO','Vale Refeição / Alimentação'],['BONIFICACOES','Bonificações'],['FERIAS','Férias'],
    ['DECIMO_TERCEIRO','13º Salário'],['RESCISOES','Rescisões'],['ENCARGOS_TRAB','Encargos Trabalhistas'],
    ['ENCARGOS_PROLABORE','Encargos sobre Pró-labore'],['REEMBOLSOS_EQUIPE','Reembolsos de Equipe'],
  ]],
  ['SERVICOS_PROJETO', 'Serviços de Projeto', [
    ['CONSULTORIA_PROJ','Consultoria de Projeto'],['FACILITACAO','Facilitação / Instrutoria'],
    ['PALESTRANTES','Palestrantes'],['MENTORES','Mentores'],['TUTORES','Tutores'],
    ['AVALIADORES','Avaliadores'],['COORD_PROJETO','Coordenação de Projeto'],
    ['APOIO_ADM_PROJ','Apoio Administrativo de Projeto'],['DESIGN_PROJETO','Design de Projeto'],
    ['COMUNICACAO_PROJ','Comunicação de Projeto'],['PRODUCAO_CONTEUDO','Produção de Conteúdo'],
    ['REVISAO_CONTEUDO','Revisão de Conteúdo'],['DESENV_MATERIAL','Desenvolvimento de Material'],
    ['SERV_TEC_ESPEC','Serviços Técnicos Especializados'],['PRESTADORES_PROJ','Prestadores Terceirizados de Projeto'],
  ]],
  ['PLATAFORMAS_PROJETO', 'Plataformas e Sistemas de Projeto', [
    ['PLAT_DESEMPENHO','Plataforma de Gestão de Desempenho'],['PLAT_APRENDIZAGEM','Plataforma de Aprendizagem'],
    ['PLAT_AVALIACAO','Plataforma de Avaliação'],['PLAT_PESQUISA','Plataforma de Pesquisa'],
    ['LIC_SW_PROJETO','Licença de Software de Projeto'],['DASHBOARD_PROJ','Sistema de Relatórios / Dashboard de Projeto'],
    ['FERR_COMUNIC_PROJ','Ferramenta de Comunicação do Projeto'],
    ['ASSIN_DIGITAL_PROJ','Assinatura Digital vinculada a Projeto'],
    ['HOSPEDAGEM_PROJ','Hospedagem ou Ambiente Digital de Projeto'],
  ]],
  ['INSTRUMENTOS_TESTES', 'Instrumentos, Testes e Avaliações', [
    ['TESTES_PSICOL','Testes Psicológicos'],['INVENTARIOS_COMP','Inventários Comportamentais'],
    ['ASSESSMENT','Assessment'],['DISC','DISC'],['AVAL_PERFIL','Avaliação de Perfil'],
    ['AVAL_COMPETENCIAS','Avaliação de Competências'],['CERTIFICACAO','Certificação de Conhecimentos'],
    ['TESTES_ONLINE','Testes Online'],['CREDITOS_TESTES','Compra de Créditos de Testes'],
    ['CORRECAO_LAUDO','Correção / Laudo / Relatório Técnico'],
  ]],
  ['VIAGENS_DESLOCAMENTOS', 'Viagens e Deslocamentos', [
    ['PASSAGEM_AEREA','Passagem Aérea'],['HOSPEDAGEM','Hospedagem'],
    ['TRANSP_APLICATIVO','Transporte por Aplicativo'],['TAXI','Táxi'],
    ['COMBUSTIVEL','Combustível'],['PEDAGIO','Pedágio'],['ESTACIONAMENTO','Estacionamento'],
    ['LOCACAO_VEICULO','Locação de Veículo'],['SEGURO_VIAGEM','Seguro Viagem'],
    ['BAGAGEM','Bagagem'],['REEMB_DESLOCAMENTO','Reembolso de Deslocamento'],
    ['ALIM_VIAGEM','Alimentação em Viagem'],['DIARIAS','Diárias de Viagem'],
  ]],
  ['EVENTOS_LOGISTICA', 'Eventos, Materiais e Logística de Projeto', [
    ['LOCACAO_ESPACO','Locação de Espaço'],['COFFEE_BREAK','Coffee Break'],
    ['ALIM_EVENTO','Alimentação de Evento'],['MATERIAL_DIDATICO','Material Didático'],
    ['APOSTILAS','Apostilas'],['IMPRESSOS','Impressos'],['BRINDES','Brindes'],
    ['KITS_PARTICIPANTES','Kits de Participantes'],['EQUIP_EVENTO','Equipamentos para Evento'],
    ['AUDIOVISUAL','Audiovisual'],['FOTO_FILMAGEM','Fotografia / Filmagem'],
    ['EQUIPE_APOIO','Equipe de Apoio'],['CREDENCIAMENTO','Credenciamento'],
    ['CORREIOS_ENTREGA','Correios / Entrega de Materiais'],['INSUMOS_PROJETO','Insumos de Projeto'],
  ]],
  ['TECNOLOGIA_INFO', 'Tecnologia da Informação', [
    ['SOFTWARE_CORP','Software Corporativo'],['LIC_SW_INTERNO','Licença de Software Interno'],
    ['HOSPEDAGEM_SITE','Hospedagem de Site'],['DOMINIO','Domínio'],
    ['EMAIL_CORP','E-mail Corporativo'],['GOOGLE_WORKSPACE','Google Workspace'],
    ['MICROSOFT_OFFICE','Microsoft / Office'],['OPENAI_CHATGPT','OpenAI / ChatGPT'],
    ['INTERNET','Internet'],['SUPORTE_TI','Suporte Técnico'],
    ['MANUT_SISTEMA','Manutenção de Sistema'],['EQUIP_INFORMATICA','Equipamentos de Informática'],
    ['SEGURANCA_DIGITAL','Segurança Digital'],['BACKUP_ARMAZEN','Backup / Armazenamento'],
  ]],
  ['ADMIN_INFRA', 'Administração e Infraestrutura', [
    ['ALUGUEL','Aluguel'],['CONDOMINIO','Condomínio'],['ENERGIA_ELETRICA','Energia Elétrica'],
    ['AGUA','Água'],['INTERNET_ESCRIT','Internet do Escritório'],
    ['MATERIAL_ESCRIT','Material de Escritório'],['MOVEIS_UTENSILIOS','Móveis e Utensílios'],
    ['MANUT_PREDIAL','Manutenção Predial'],['LIMPEZA','Limpeza'],['CORREIOS','Correios'],
    ['CARTORIO','Cartório'],['CERT_DIGITAL','Certificado Digital'],
    ['ASSIN_ADM','Assinaturas Administrativas'],['DESP_GERAIS_ADM','Despesas Gerais Administrativas'],
  ]],
  ['CONTABILIDADE_FISCAL', 'Contabilidade e Fiscal', [
    ['HONOR_CONTABEIS','Honorários Contábeis'],['ASSESSORIA_CONT','Assessoria Contábil'],
    ['OBRIG_ACESSORIAS','Obrigações Acessórias'],['REGULAR_FISCAIS','Regularizações Fiscais'],
    ['CERTIDOES','Certidões'],['DP_CONTABIL','Serviços de Departamento Pessoal Contábil'],
    ['CONSULT_FISCAL','Consultoria Fiscal'],
  ]],
  ['JURIDICO', 'Jurídico', [
    ['HONOR_JURIDICOS','Honorários Jurídicos'],['ASSESSORIA_JUR','Assessoria Jurídica'],
    ['ELAB_CONTRATOS','Elaboração de Contratos'],['ANALISE_CONTRAT','Análise Contratual'],
    ['PROCESSOS_JUD','Processos Judiciais'],['CUSTAS_JUD','Custas Judiciais'],
    ['CARTORIO_JUR','Cartório Jurídico'],['CONSULT_TRAB','Consultoria Trabalhista'],
    ['CONSULT_SOCIE','Consultoria Societária'],
  ]],
  ['MARKETING_COMUNICACAO', 'Marketing e Comunicação', [
    ['PUBLICIDADE','Publicidade'],['GOOGLE_ADS','Google Ads'],['REDES_SOCIAIS','Redes Sociais'],
    ['DESIGN_INSTIT','Design Institucional'],['COMUNIC_INSTIT','Comunicação Institucional'],
    ['SITE_INSTIT','Site Institucional'],['PROD_CONT_COMERC','Produção de Conteúdo Comercial'],
    ['MATERIAL_COMERC','Material Comercial'],['APRES_COMERCIAIS','Apresentações Comerciais'],
    ['IDENTIDADE_VISUAL','Identidade Visual'],['ASSESSORIA_COMUNIC','Assessoria de Comunicação'],
  ]],
  ['TRIBUTOS_IMPOSTOS', 'Tributos e Impostos', [
    ['DAS','DAS'],['ISS','ISS'],['IRPJ','IRPJ'],['CSLL','CSLL'],['PIS','PIS'],
    ['COFINS','COFINS'],['IRRF','IRRF'],['INSS','INSS'],['FGTS','FGTS'],
    ['IMP_FEDERAIS','Impostos Federais'],['IMP_MUNICIPAIS','Impostos Municipais'],
    ['MULTAS_FISCAIS','Multas Fiscais'],['JUROS_FISCAIS','Juros Fiscais'],
  ]],
  ['TRIBUTOS_FATURAMENTO', 'Tributos sobre Faturamento', [
    ['DAS_NF','DAS sobre NF'],['ISS_NF','ISS sobre NF'],['IRRF_RETIDO','IRRF Retido'],
    ['PIS_RETIDO','PIS Retido'],['COFINS_RETIDO','COFINS Retido'],['CSLL_RETIDA','CSLL Retida'],
    ['INSS_RETIDO','INSS Retido'],['RETENCOES_CLI','Retenções de Cliente'],
    ['IMP_RECEITA_PROJ','Imposto sobre Receita de Projeto'],
  ]],
  ['DESPESAS_BANCARIAS', 'Despesas Bancárias e Financeiras', [
    ['TARIFA_BANCARIA','Tarifa Bancária'],['PACOTE_BANCARIO','Pacote de Serviços Bancários'],
    ['TARIFA_PIX','Tarifa PIX'],['TARIFA_TED_DOC','Tarifa TED / DOC'],
    ['TARIFA_COBRANCA','Tarifa de Cobrança'],['JUROS_BANC','Juros Bancários'],
    ['MULTA_BANC','Multa Bancária'],['MANUT_CONTA','Manutenção de Conta'],
    ['ANUIDADE_CARTAO','Anuidade de Cartão'],['TAXA_CARTAO','Taxa de Cartão'],
    ['DESP_FIN_DIVERSAS','Despesas Financeiras Diversas'],
  ]],
  ['IOF', 'IOF', [
    ['IOF_BANCARIO','IOF sobre Operação Bancária'],['IOF_INTERNACIONAL','IOF sobre Compra Internacional'],
    ['IOF_EMPRESTIMO','IOF sobre Empréstimo'],['IOF_APLICACAO','IOF sobre Aplicação'],
    ['IOF_CARTAO','IOF sobre Cartão'],
  ]],
  ['EMPRESTIMOS_MUTUOS', 'Empréstimos, Mútuos e Financiamentos', [
    ['MUTUO_ENTRADA','Mútuo — Entrada'],['MUTUO_SAIDA','Mútuo — Saída'],
    ['EMPRESTIMO_BANC','Empréstimo Bancário'],['PRONAMPE','Pronampe'],
    ['AMORTIZACAO','Amortização de Empréstimo'],['JUROS_EMPRESTIMO','Juros de Empréstimo'],
    ['ENCARGOS_EMPREST','Encargos de Empréstimo'],
  ]],
  ['TRANSFERENCIAS', 'Transferências e Movimentações entre Contas', [
    ['TRANSF_CONTAS','Transferência entre Contas'],['APLICACAO_FIN','Aplicação Financeira'],
    ['RESGATE_APLIC','Resgate de Aplicação'],['MOV_BANCOS','Movimentação entre Bancos'],
    ['AJUSTE_CAIXA','Ajuste de Caixa'],['SALDO_INICIAL','Saldo Inicial / Saldo Atual'],
  ]],
  ['ESTORNOS_REEMBOLSOS', 'Estornos, Reembolsos e Recuperações', [
    ['ESTORNO_DESPESA','Estorno de Despesa'],['ESTORNO_CARTAO','Estorno de Cartão'],
    ['REEMB_RECEBIDO','Reembolso Recebido'],['REEMB_PAGO','Reembolso Pago'],
    ['DEVOL_TAXA','Devolução de Taxa'],['DEVOL_CAUCAO','Devolução de Caução'],
    ['RECUPERACAO_DESP','Recuperação de Despesa'],['AJUSTE_LANCAMENTO','Ajuste de Lançamento'],
  ]],
  ['SEGUROS', 'Seguros', [
    ['SEGURO_EMPRESARIAL','Seguro Empresarial'],['SEGURO_VIDA','Seguro de Vida'],
    ['SEGURO_VIAGEM_SEG','Seguro Viagem'],['SEGURO_EQUIP','Seguro Equipamentos'],
    ['SEGURO_RC','Seguro Responsabilidade Civil'],['SEGURO_SAUDE','Seguro Saúde'],
  ]],
  ['A_CLASSIFICAR', 'A Classificar', [
    ['SEM_INFO','Sem Informação Suficiente'],['DESC_INCOMPLETA','Descrição Incompleta'],
    ['CC_CONFLITANTE','Centro de Custo Conflitante'],['CLI_NAO_IDENT','Cliente não Identificado'],
    ['FORN_NAO_IDENT','Fornecedor não Identificado'],['LANC_ZERADO','Lançamento Zerado a Revisar'],
    ['STATUS_REVISAR','Status a Revisar'],
  ]],
];

async function seed() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM tipos_despesa');
    await client.query('DELETE FROM grupos_despesa');

    let totalTipos = 0;
    for (const [gcod, gnome, tipos] of ESTRUTURA) {
      const gr = await client.query(
        'INSERT INTO grupos_despesa (codigo, nome, ativo) VALUES ($1, $2, true) RETURNING id',
        [gcod, gnome]
      );
      const gid = gr.rows[0].id;
      for (const [tcod, tnome] of tipos) {
        await client.query(
          'INSERT INTO tipos_despesa (codigo, nome, grupo_id, grupo_codigo, ativo) VALUES ($1, $2, $3, $4, true)',
          [tcod, tnome, gid, gcod]
        );
        totalTipos++;
      }
    }

    await client.query('COMMIT');
    console.log(`✅ Inseridos: ${ESTRUTURA.length} grupos e ${totalTipos} tipos de despesa.`);

    // Verificação
    const r = await client.query(`
      SELECT g.nome, COUNT(t.id) as qtd
      FROM grupos_despesa g
      LEFT JOIN tipos_despesa t ON t.grupo_id = g.id
      GROUP BY g.id, g.nome ORDER BY g.id
    `);
    for (const row of r.rows) {
      console.log(`  ${row.nome.padEnd(50)} ${row.qtd} tipos`);
    }
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('ERRO:', err.message);
  } finally {
    client.release();
    await pool.end();
  }
}

seed();
