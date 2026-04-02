import { AuditData } from '../types';
import { formatCurrency, getCenterPoint } from '../utils';

// Mapeamento exato do seu SQL
export const producerSizeMap: Record<string, string> = {
  'MINIFÚNDIO': 'Minifúndio',
  'PEQUENA PROPRIEDADE': 'Pequena Propriedade',
  'MÉDIA PROPRIEDADE': 'Média Propriedade',
  'GRANDE PROPRIEDADE': 'Grande Propriedade',
  'NÃO CLASSIFICADO': 'Não Classificado'
};

// Cores técnicas para os badges de tamanho
export const getSizeBadgeStyle = (size: string) => {
  switch (size) {
    case 'MINIFÚNDIO': return 'bg-blue-50 text-blue-700 border-blue-100';
    case 'PEQUENA PROPRIEDADE': return 'bg-emerald-50 text-emerald-700 border-emerald-100';
    case 'MÉDIA PROPRIEDADE': return 'bg-orange-50 text-orange-700 border-orange-100';
    case 'GRANDE PROPRIEDADE': return 'bg-slate-900 text-white border-slate-800';
    default: return 'bg-slate-100 text-slate-600 border-slate-200';
  }
};

export const carStatusMap: Record<string, string> = {
  'AT': 'ATIVO',
  'PE': 'PENDENTE',
  'SU': 'SUSPENSO',
  'ATIVO': 'ATIVO'
};

export const formattedDate = (d: any) => {
  if (!d || d === 'None' || d === '1900-01-01' || d === '') return 'Não identificada';
  
  try {
    const date = new Date(d);
    if (isNaN(date.getTime())) return 'Não identificada';
    
    return date.toLocaleDateString('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      timeZone: 'UTC'
    });
  } catch (e) {
    return 'Não identificada';
  }
};

// lib/audit-utils.ts

export const getAnalysisReason = (status: string, confidence: string) => {
  const s = status.toUpperCase();

  // 1. IDENTIDADES (PRODUTORES ESPECIAIS)
  if (s.includes('SETTLEMENT PRODUCER')) return "Produtor Assentado: Imóvel em área de assentamento com ocupação regularizada identificada.";
  if (s.includes('TRADITIONAL PRODUCER')) return "Comunidade Tradicional: Território ocupado por população tradicional com reconhecimento espacial.";
  if (s.includes('QUILOMBOLA PRODUCER')) return "Produtor Quilombola: Imóvel em território quilombola com identidade reconhecida.";

  // 2. BLOQUEIOS SOCIAIS E TERRITORIAIS
  if (s.includes('SOCIAL RISK') || s.includes('SLAVE LABOR')) return "Violação Social: Titularidade vinculada ao Cadastro de Empregadores (Lista Suja do Trabalho Escravo).";
  if (s.includes('INDIGENOUS LAND')) return "Restrição Crítica: O imóvel sobrepõe Terra Indígena homologada ou em estudo.";
  if (s.includes('CONSERVATION UNIT')) return "Restrição Ambiental: Sobreposição detectada com Unidade de Conservação de Proteção Integral.";
  if (s.includes('QUILOMBOLA (INVASION)')) return "Conflito Territorial: Sobreposição não autorizada com Território Quilombola.";
  if (s.includes('SETTLEMENT (INVASION)')) return "Conflito Fundiário: Sobreposição detectada com Assentamento Rural (INCRA).";
  
  // 3. BLOQUEIOS POR DESMATAMENTO E EUDR
  if (s.includes('APP DEFORESTATION')) return "Inconformidade Legal: Supressão de vegetação nativa em Área de Preservação Permanente (APP).";
  if (s.includes('EUDR VIOLATION')) return "Inconformidade EUDR: Imóvel com restrição de exportação para a União Europeia devido a desmatamento pós-2020.";
  if (s.includes('DEFORESTATION')) return "Inconformidade Ambiental: Supressão de vegetação nativa detectada e validada via monitoramento satelital.";
  
  // 4. EMBARGOS (INCLUINDO ÓRGÃOS ESPECÍFICOS DO SEU SQL: SEMA, SIGA, IBAMA)
  if (s.includes('EMBARGO') || s.includes('SEMA_MT') || s.includes('SIGA_MT') || s.includes('ICMBIO') || s.includes('IBAMA')) {
    return "Embargo Administrativo: Restrição ativa vinculada a órgãos fiscalizadores (IBAMA, SEMA-MT ou ICMBio).";
  }
  
  // 5. BLOQUEIOS TÉCNICOS
  if (s.includes('CAR STATUS')) return "Irregularidade Cadastral: O registro do CAR encontra-se Cancelado ou Suspenso no sistema nacional.";
  if (s.includes('INVALID GEOMETRY')) return "Erro Técnico: Geometria do imóvel inválida ou inexistente para processamento automático.";
  if (s.includes('SATELLITE (SLOPE)')) return "Restrição Técnica: Declividade do terreno superior ao limite permitido para exploração segura.";

  // 6. REVISÕES E ALERTAS
  if (s.includes('UNUSUAL AREA')) return "Revisão Obrigatória: Dimensão do imóvel incompatível com os limites usuais para esta categoria de produtor.";
  if (s.includes('RISK BY ADJACENCY')) return "Risco por Adjacência: Identificado passivo ambiental crítico em imóvel confrontante com alto potencial de contágio.";
  if (s.includes('WARNING') || s.includes('CONDITIONAL')) return "Conformidade Condicional: Identificados alertas ou déficits ambientais que requerem regularização.";

  // 7. SALVA-VIDAS PARA QUALQUER "NOT ELIGIBLE" NÃO MAPEADO ACIMA
  if (s.startsWith('NOT ELIGIBLE')) {
    return "Inconformidade Detectada: O imóvel apresenta restrições socioambientais críticas que impedem a elegibilidade.";
  }

  // 8. CASOS DE SUCESSO OU INCERTEZA
  if (confidence.toUpperCase().includes('LOW')) return "Incerteza Cartográfica: O imóvel requer validação documental devido ao baixo nível de confiança na base geoespacial.";
  if (s === 'ELIGIBLE') return "Análise concluída: O imóvel atende aos critérios de conformidade socioambiental vigentes.";

  return "Análise de Risco: Verifique as evidências detalhadas abaixo para o parecer final de conformidade.";
};

export const determineStatusColor = (statusRaw: string, confidence: string): 'red' | 'orange' | 'green' | 'blue' => {
  const status = statusRaw.toUpperCase();
  const conf = confidence.toUpperCase();

  // 1. VERMELHO: Bloqueio explícito
  if (status.includes('NOT ELIGIBLE')) return 'red';

  // 2. LARANJA: Alertas e Condicionais
  if (status.includes('WARNING') || status.includes('CONDITIONAL')) return 'orange';

  // 3. AZUL: Revisão Manual OU Elegível com Baixa Confiança
  if (status.includes('MANUAL_REVIEW') || status.includes('AWAITING') || conf.includes('LOW')) {
    return 'blue';
  }

  // 4. VERDE: Elegível com Confiança Alta/Média
  if (status.startsWith('ELIGIBLE')) return 'green';

  return 'blue';
};

export const deforestationTypeMap: Record<string, string> = {
  'agriculture': 'Agricultura',
  'pasture': 'Pastagem',
  'mining': 'Mineração',
  'ilegal_mining': 'Mineração Ilegal',
  'other': 'Outros / Não Identificado',
  'others': 'Outros / Não Identificado',
  'natural_regeneration': 'Regeneração Natural',
  'infrastructure': 'Infraestrutura',
  'fire': 'Queimada / Incêndio',
  'forestry': 'Silvicultura'
};

export const translateDeforestationTypes = (types: string) => {
  if (!types || types === 'N/A') return 'Não especificada';
  // O segredo está no split(/[| ,]+/) que aceita tanto vírgula quanto a barra |
  return types.split(/[| ,]+/)
    .map(t => t.trim().toLowerCase())
    .filter(t => t !== "")
    .map(t => deforestationTypeMap[t] || t) 
    .join(', ');
};

export const confidenceMap: Record<string, string> = {
  'HIGH_CONFIDENCE': 'Alta Precisão',
  'MEDIUM_CONFIDENCE': 'Precisão Moderada',
  'LOW_CONFIDENCE': 'Baixa Precisão',
  'VECTOR ERROR (PROTECTED AREA)': 'Inconsistência de Vetor em Área Protegida',
  'MICRO EMBARGO': 'Embargo de Extensão Irrelevante (Ruído)',
  'MICRO DEFORESTATION': 'Supressão de Extensão Irrelevante (Ruído)',
  'SENSOR NOISE (SLOPE)': 'Ruído de Sensor em Declividade Elevada',
  'BOUNDARY DISPUTE': 'Conflito de Limites Geográficos',
  'STATE VERIFIED': 'Validado pela Base Estadual (Premium)',
  'VALID SPATIAL INTERSECTION': 'Cruzamento Espacial Validado',
  'OVERLAP': 'Sobreposição de Perímetros',
  'INCONSISTENT DATA': 'Dados Cadastrais Inconsistentes',
  'OUTDATED IMAGERY': 'Defasagem de Imagens Satelitais',
  'MANUAL_REVIEW': 'Necessita Revisão Técnica',
  'N/A': 'Não Avaliado'
};

export const translateConfidence = (confidenceRaw: string) => {
  if (!confidenceRaw || confidenceRaw === 'N/A') return 'Análise em Processamento';

  return confidenceRaw.split(/[-]+/)
    .map(part => part.trim().toUpperCase())
    .map(part => confidenceMap[part] || part) 
    .join(': ');
};

export const formatListFromSql = (text: string | null) => {
  if (!text || text === 'None' || text === '') return [];
  return text.split('|').map(item => item.trim());
};

export const getStatusBadge = (isTechnicallyBlocked: boolean, status: string) => {
  // Se o status for NOT ELIGIBLE ou se a flag de bloqueio técnico for TRUE
  if (status.includes('NOT ELIGIBLE') || isTechnicallyBlocked) {
    return { label: 'OPERAÇÃO BLOQUEADA', color: 'red' };
  }
  
  if (status.includes('PRODUCER')) {
    return { label: 'PRODUTOR REGULARIZADO', color: 'green' };
  }

  if (status.includes('MANUAL_REVIEW') || status.includes('AWAITING')) {
    return { label: 'REVISÃO TÉCNICA OBRIGATÓRIA', color: 'blue' };
  }

  if (status === 'ELIGIBLE') {
    return { label: 'COMPLIANCE VERIFICADO', color: 'green' };
  }

  return { label: 'EM ANÁLISE', color: 'blue' };
};

export const statusLabelMap: Record<string, string> = {
  'ELIGIBLE': 'CONFORME',
  'NOT ELIGIBLE': 'BLOQUEADO',
  'WARNING': 'ALERTA',
  'CONDITIONAL': 'CONDICIONAL',
  'MANUAL_REVIEW': 'REVISÃO',
  'MANUAL_REVIEW_REQUIRED': 'REVISÃO',
  'AWAITING_MANUAL_VALIDATION': 'AGUARDANDO',
  'UNKNOWN': 'NÃO IDENTIFICADO'
};

// Função para pegar apenas o rótulo traduzido
export const translateStatus = (statusRaw: string) => {
  const mainStatus = statusRaw.split(' - ')[0].toUpperCase();
  return statusLabelMap[mainStatus] || mainStatus;
};