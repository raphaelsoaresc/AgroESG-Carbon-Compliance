import { AuditData } from '../types';
import { formatCurrency, getCenterPoint } from '../utils';

/**
 * 1. MAPEAMENTOS DE TRADUÇÃO (DICIONÁRIOS)
 */

export const producerSizeMap: Record<string, string> = {
  'MINIFÚNDIO': 'Minifúndio',
  'PEQUENA PROPRIEDADE': 'Pequena Propriedade',
  'MÉDIA PROPRIEDADE': 'Média Propriedade',
  'GRANDE PROPRIEDADE': 'Grande Propriedade',
  'NÃO CLASSIFICADO': 'Não Classificado'
};

export const reliefMap: Record<string, string> = {
  'FLAT': 'Plano',
  'GENTLE': 'Suave Ondulado',
  'UNDULATING': 'Ondulado',
  'STRONGLY_UNDULATING': 'Forte Ondulado',
  'MOUNTAINOUS': 'Montanhoso',
  'ESCARPMENT': 'Escarpado'
};

export const logisticsRiskMap: Record<string, string> = {
  'CRITICAL': 'Risco Crítico',
  'HIGH': 'Risco Alto',
  'MEDIUM': 'Risco Moderado',
  'LOW': 'Risco Baixo',
  'NEGLIGIBLE': 'Risco Irrelevante'
};

export const rlStatusMap: Record<string, string> = {
  'CONFORME': 'Regular',
  'DEFICIT': 'Déficit de Reserva',
  'EM_REGULARIZACAO': 'Em Regularização (PRA)',
  'NAO_APLICAVEL': 'Não Aplicável'
};

export const confidenceMap: Record<string, string> = {
  'ULTRA_HIGH': 'Precisão Máxima',
  'HIGH_CONFIDENCE': 'Alta Precisão',
  'MEDIUM_CONFIDENCE': 'Precisão Moderada',
  'LOW_CONFIDENCE': 'Baixa Precisão',
  'DOUBLE_VERIFIED': 'Dupla Verificação',
  'GIS': 'Satelital',
  'MAPBIOMAS': 'MapBiomas',
  'VECTOR ERROR': 'Erro de Vetor',
  'PROTECTED AREA': 'Área Protegida',
  'MICRO EMBARGO': 'Embargo Irrelevante',
  'MICRO DEFORESTATION': 'Supressão Irrelevante',
  'SENSOR NOISE': 'Ruído de Sensor',
  'SLOPE': 'Declividade (Sensor)',
  'BOUNDARY DISPUTE': 'Conflito de Limites',
  'STATE VERIFIED': 'Validado pelo Estado',
  'VALID SPATIAL INTERSECTION': 'Cruzamento Validado',
  'OVERLAP': 'Sobreposição',
  'MANUAL_REVIEW': 'Revisão Técnica',
  'N/A': 'Não Avaliado'
};

export const mapbiomasClassMap: Record<string, string> = {
  'forest': 'Floresta',
  'savanna': 'Formação Savânica',
  'mangrove': 'Manguezal',
  'wetland': 'Área Úmida',
  'grassland': 'Formação Campestre',
  'pasture': 'Pastagem',
  'agriculture': 'Agricultura',
  'perennial_crop': 'Cultura Perpétua',
  'non_vegetated': 'Área sem Vegetação',
  'mining': 'Mineração',
  'fire': 'Queimada',
  'infrastructure': 'Infraestrutura',
  'others': 'Outro Uso',
  'water': 'Corpo d\'Água',
  'urban': 'Área Urbana',
  'snow_ice': 'Neve/Gelo',
  'clouds': 'Nuvens',
  'no_data': 'Sem Dados',
  'ilegal_mining': 'Mineração Ilegal',
  'deforestation': 'Desmatamento',
  'degradation': 'Degradação',
  'regeneration': 'Regeneração',
  'reforestation': 'Reflorestamento',
  'silviculture': 'Silvicultura',
  'other_vegetation': 'Outra Vegetação'
};

/**
 * 2. ESTILIZAÇÃO DE UI (BADGES E CORES)
 */

export const getSectionStatusStyle = (status: string | undefined | null) => {
  const s = status?.toUpperCase() || '';
  
  // AMARELO/ORANGE: Alertas, Avisos e Pendências
  if (s.includes('PENDENTE') || s.includes('ANÁLISE') || s.includes('WARNING') || s.includes('ALERTA') || s.includes('CONDITIONAL')) {
    return 'bg-amber-50 text-amber-700 border-amber-200';
  }
  
  // VERMELHO: Bloqueios e Inconsistências
  if (s.includes('CANCELADO') || s.includes('SUSPENSO') || s.includes('INCONSISTENTE') || s.includes('BLOQUEADO')) {
    return 'bg-red-50 text-red-700 border-red-200';
  }
  
  // VERDE: Regularidade
  if (s.includes('ATIVO') || s.includes('REGULAR') || s.includes('CONFORME')) {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  }

  // AZUL: Manual Review e Padrão
  if (s.includes('MANUAL') || s.includes('REVIEW')) {
    return 'bg-blue-50 text-blue-700 border-blue-200';
  }
  
  return 'bg-slate-50 text-slate-600 border-slate-200';
};

export const getSizeBadgeStyle = (size: string) => {
  const s = size?.toUpperCase() || '';
  if (s.includes('MINIFÚNDIO')) return 'bg-slate-50 text-slate-600 border-slate-200';
  if (s.includes('PEQUENA')) return 'bg-slate-100 text-slate-700 border-slate-300'; 
  if (s.includes('MÉDIA')) return 'bg-slate-200 text-slate-800 border-slate-400';
  if (s.includes('GRANDE')) return 'bg-slate-900 text-white border-slate-900';
  return 'bg-white text-slate-400 border-slate-100';
};

export const determineStatusColor = (statusRaw: string, confidenceRaw?: string): 'red' | 'orange' | 'green' | 'blue' => {
  if (!statusRaw) return 'blue';
  const s = statusRaw.toUpperCase();
  
  if (s.includes('NOT ELIGIBLE') || s.includes('BLOQUEADO')) return 'red';
  
  // WARNING/ALERTA/CONDITIONAL -> ORANGE (AMARELO)
  if (
    s.includes('WARNING') || 
    s.includes('CONDITIONAL') || 
    s.includes('PENDENTE') || 
    s.includes('ALERTA') || 
    s.includes('RISK') || 
    s.includes('ADJACENCY')
  ) return 'orange';
  
  if (s.startsWith('ELIGIBLE') || s.includes('CONFORME')) return 'green';
  
  // MANUAL REVIEW -> BLUE
  if (s.includes('MANUAL') || s.includes('REVIEW')) return 'blue';
  
  return 'blue';
};

export const getStatusBadge = (isTechnicallyBlocked: boolean, status: string) => {
  const s = status?.toUpperCase() || '';
  if (s.includes('NOT ELIGIBLE') || isTechnicallyBlocked) return { label: 'OPERAÇÃO BLOQUEADA', color: 'red' };
  if (s.includes('PRODUCER')) return { label: 'PRODUTOR REGULARIZADO', color: 'green' };
  
  // MANUAL REVIEW -> BLUE
  if (s.includes('MANUAL_REVIEW')) return { label: 'REVISÃO OBRIGATÓRIA', color: 'blue' };
  
  // WARNING/ALERTA/CONDITIONAL -> ORANGE
  if (s.includes('WARNING') || s.includes('ALERTA') || s.includes('ADJACENCY') || s.includes('CONDITIONAL')) return { label: 'ALERTA DE COMPLIANCE', color: 'orange' };

  if (s.startsWith('ELIGIBLE')) return { label: 'COMPLIANCE VERIFICADO', color: 'green' };
  
  return { label: 'EM ANÁLISE', color: 'blue' };
};

/**
 * 3. LÓGICA DE TRADUÇÃO E FORMATAÇÃO
 */

export const translateConfidence = (confidenceRaw: string) => {
  if (!confidenceRaw || confidenceRaw === 'N/A') return 'Análise em Processamento';
  return confidenceRaw
    .split(/[\s\-_()+|]+/)
    .map(part => part.trim().toUpperCase())
    .filter(part => part !== "" && part !== "STATUS" && part !== "CONFIDANÇA")
    .map(part => confidenceMap[part] || part) 
    .join(' ');
};

export const translateStatus = (statusRaw: string) => {
  if (!statusRaw) return 'NÃO IDENTIFICADO';
  const s = statusRaw.toUpperCase();
  if (s.includes('NOT ELIGIBLE')) return 'BLOQUEADO';
  if (s.includes('ELIGIBLE')) return 'CONFORME';
  if (s.includes('MANUAL_REVIEW')) return 'NECESSITA REVISÃO MANUAL';
  if (s.includes('WARNING')) return 'ALERTA';
  if (s.includes('CONDITIONAL')) return 'CONDICIONAL';
  return s;
};

export const translateDeforestationTypes = (types: string) => {
  if (!types || types === 'N/A') return 'Não especificada';
  return types
    .split(/[| ,;]+/)
    .map(t => mapbiomasClassMap[t.trim().toLowerCase()] || t)
    .join(', ');
};

/**
 * 4. MOTIVO DA ANÁLISE (ALINHADO COM O SQL)
 */
export const getAnalysisReason = (status: string, confidence?: string) => {
  if (!status) return "Análise de Risco: Verifique as evidências detalhadas abaixo.";
  const s = status.toUpperCase();

  // Bloqueios Críticos (Nível 1 e 2 do SQL)
  if (s.includes('SLAVE LABOR')) return "Violação Social: Titularidade vinculada à Lista Suja do Trabalho Escravo (MTE).";
  if (s.includes('AMZ EMBARGO') || s.includes('5.081')) return "Restrição Crítica (CMN 5.081): Embargo em bioma Amazônia detectado.";
  if (s.includes('EMBARGO')) return "Embargo Administrativo: Restrição ativa vinculada a órgãos fiscalizadores (IBAMA/SEMA).";
  if (s.includes('DEFORESTATION (MAPBIOMAS)')) return "Inconformidade Ambiental: Supressão de vegetação nativa detectada pelo MapBiomas.";
  if (s.includes('EUDR VIOLATION')) return "Inconformidade EUDR: Restrição de exportação por desmatamento pós-2020.";
  if (s.includes('APP DEFORESTATION')) return "Inconformidade Legal: Supressão de vegetação em Área de Preservação Permanente (APP).";
  if (s.includes('STRUCTURED ENVIRONMENTAL RISK')) return "Risco Estruturado: Associação de desmatamento com infraestrutura logística crítica.";
  
  // Invasões Territoriais
  if (s.includes('INDIGENOUS LAND')) return "Restrição Territorial: O imóvel sobrepõe Terra Indígena homologada.";
  if (s.includes('CONSERVATION UNIT')) return "Restrição Territorial: Sobreposição com Unidade de Conservação.";
  if (s.includes('QUILOMBOLA (INVASION)')) return "Restrição Territorial: Sobreposição com Território Quilombola.";
  if (s.includes('SETTLEMENT (INVASION)')) return "Restrição Territorial: Sobreposição com Assentamento Incra.";
  
  // Jurídico e Técnico
  if (s.includes('CAR STATUS')) return "Irregularidade Cadastral: O registro do CAR encontra-se Cancelado ou Suspenso.";
  if (s.includes('INVALID GEOMETRY')) return "Erro Técnico: Geometria do imóvel inválida ou ausente.";
  if (s.includes('POTENTIAL AREA FRAUD')) return "Alerta de Fraude: Divergência crítica (>50%) entre área declarada e geometria.";
  if (s.includes('INCONSISTENT AREA')) return "Inconsistência de Dados: Área processada diverge do valor declarado na origem.";
  
  // Identidades Especiais (Elegíveis)
  if (s.includes('INDIGENOUS PRODUCER')) return "Produtor Indígena: Imóvel em TI com manejo autorizado.";
  if (s.includes('SETTLEMENT PRODUCER')) return "Produtor Assentado: Imóvel em área de assentamento regularizado.";
  if (s.includes('QUILOMBOLA PRODUCER')) return "Produtor Quilombola: Imóvel em território quilombola reconhecido.";

  // Riscos Indiretos e Alertas
  if (s.includes('MITIGATED ADJACENCY')) return "Risco de Adjacência Mitigado: Existe barreira física (rio/estrada) protegendo o imóvel.";
  if (s.includes('LAUNDERING')) return "Risco de Lavagem: Proximidade crítica com áreas de desmatamento (Triangulação).";
  if (s.includes('RISK BY ADJACENCY')) return "Risco por Adjacência: Passivo ambiental crítico em imóvel confrontante.";
  if (s.includes('RL DEFICIT')) return "Déficit de Reserva Legal: Área de vegetação inferior ao exigido pelo Código Florestal.";

  if (s.includes('NOT ELIGIBLE')) return "Inconformidade Detectada: O imóvel apresenta restrições socioambientais impeditivas.";
  if (s.includes('ELIGIBLE')) return "Conformidade Verificada: O imóvel atende aos critérios socioambientais e normativos.";

  return "Análise de Risco: Verifique as evidências detalhadas abaixo.";
};

/**
 * 5. AUXILIARES DE FORMATAÇÃO DE DADOS
 */

export const formattedDate = (d: any) => {
  if (!d || d === 'None' || d === '1900-01-01' || d === '') return 'Não identificada';
  try {
    const date = new Date(d);
    return isNaN(date.getTime()) ? 'Não identificada' : date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', timeZone: 'UTC' });
  } catch { return 'Não identificada'; }
};

export const formatEvidenceList = (evidenceString: string): string[] => {
  if (!evidenceString) return [];
  return evidenceString.split('|').map(s => s.trim()).filter(s => s !== '');
};

export const formatLiability = (value: number) => {
  if (!value || value <= 0) return 'R$ 0,00';
  return formatCurrency(value);
};