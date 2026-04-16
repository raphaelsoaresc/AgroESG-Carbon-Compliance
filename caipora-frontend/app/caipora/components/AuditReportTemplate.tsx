import React from 'react';
import { Page, Text, View, Document, StyleSheet, Image } from '@react-pdf/renderer';
import { AuditData } from '../types';
import { 
  formattedDate, 
  translateConfidence, 
  getAnalysisReason,
  producerSizeMap,
  reliefMap,
  logisticsRiskMap,
  rlStatusMap,
  getStatusBadge,
  translateStatus,
  translateDeforestationTypes,
  formatLiability
} from '../lib/audit-utils';

const styles = StyleSheet.create({
  page: { 
    padding: 30, 
    fontSize: 7, 
    fontFamily: 'Helvetica', 
    color: '#1e293b', 
    backgroundColor: '#ffffff',
    display: 'flex',
    flexDirection: 'column'
  },
  
  header: { 
    borderBottom: '2pt solid #0f172a', 
    paddingBottom: 8, 
    marginBottom: 10, 
    flexDirection: 'row', 
    justifyContent: 'space-between', 
    alignItems: 'center',
    minHeight: 50 // Garante espaço para as logos
  },
  logoAgri: { width: 100, height: 'auto' },
  logoCaipora: { width: 45, height: 'auto', borderRadius: 4 },
  headerInfo: { textAlign: 'center', flex: 1 },
  reportTitle: { fontSize: 12, fontWeight: 'bold', color: '#0f172a', textTransform: 'uppercase' },
  engineTag: { fontSize: 6, color: '#64748b', marginTop: 2 },

  statusBanner: { padding: 10, borderRadius: 4, marginBottom: 12, flexDirection: 'row', justifyContent: 'space-between' },
  statusText: { fontSize: 11, fontWeight: 'bold', color: '#ffffff', textTransform: 'uppercase' },
  confidenceText: { fontSize: 8, color: '#ffffff', opacity: 0.9 },

  section: { 
    marginBottom: 10, 
    padding: 6, 
    border: '0.5pt solid #e2e8f0', 
    borderRadius: 4,
    // Força o bloco a ser tratado como uma unidade única
    break: false, 
  },
  sectionTitle: { fontSize: 8, fontWeight: 'bold', color: '#ffffff', marginBottom: 6, textTransform: 'uppercase', backgroundColor: '#0f172a', padding: 3 },
  
  grid: { flexDirection: 'row', flexWrap: 'wrap' },
  // Grid sem wrap para áreas sensíveis que não podem quebrar internamente
  gridNoWrap: { flexDirection: 'row', flexWrap: 'nowrap' }, 
  
  col3: { width: '33.3%', marginBottom: 4, paddingRight: 4 },
  col4: { width: '25%', marginBottom: 4, paddingRight: 4 },
  col2: { width: '50%', marginBottom: 4, paddingRight: 4 },
  
  label: { fontSize: 5.5, color: '#64748b', textTransform: 'uppercase', fontWeight: 'bold', marginBottom: 1 },
  value: { fontSize: 7, fontWeight: 'medium', color: '#1e293b' },
  valueBold: { fontSize: 7, fontWeight: 'bold', color: '#0f172a' },
  
  highlightBox: { backgroundColor: '#f8fafc', padding: 5, borderRadius: 2, marginTop: 4, border: '0.5pt solid #f1f5f9' },
  criticalBox: { backgroundColor: '#fef2f2', borderLeft: '3pt solid #ef4444', padding: 6, marginTop: 4 },
  warningBox: { backgroundColor: '#fffbeb', borderLeft: '3pt solid #f59e0b', padding: 6, marginTop: 4 },

  successBox: { backgroundColor: '#f0fdf4', borderLeft: '3pt solid #15803d', padding: 8, marginTop: 6, marginBottom: 4 },
  successText: { color: '#15803d', fontSize: 8, fontWeight: 'bold' },
  
  mapContainer: { 
    marginTop: 5, 
    marginBottom: 10, 
    border: '2pt solid #0f172a', 
    borderRadius: 8, 
    backgroundColor: '#f8fafc',
    overflow: 'hidden',
    width: 320, 
    height: 260, // Reduzido de 320 para 260 para evitar empurrar seções para a quebra
    alignSelf: 'center' 
  },
  mapImage: { width: '100%', height: '100%', objectFit: 'cover' },
  mapCaption: { fontSize: 7, color: '#ffffff', backgroundColor: '#0f172a', padding: 5, textAlign: 'center', fontWeight: 'bold', textTransform: 'uppercase' },

  tableHeader: { flexDirection: 'row', backgroundColor: '#f1f5f9', padding: 4, borderBottom: '0.5pt solid #cbd5e1', marginTop: 5 },
  tableRow: { flexDirection: 'row', padding: 4, borderBottom: '0.1pt solid #e2e8f0' },
  tableCell: { fontSize: 6 },

  evidenceTag: { fontSize: 6, padding: 3, marginRight: 4, marginBottom: 4, borderRadius: 3, border: '0.5pt solid #cbd5e1' },

  footer: { position: 'absolute', bottom: 20, left: 30, right: 30, borderTop: '0.5pt solid #e2e8f0', paddingTop: 8 },
  footerText: { fontSize: 6, color: '#94a3b8', textAlign: 'center' }
});

const AuditReportTemplate = ({ data }: { data: AuditData }) => {
  const statusColors = { red: '#b91c1c', orange: '#d97706', green: '#15803d', blue: '#1d4ed8' };
  const badgeInfo = getStatusBadge(data.is_technically_blocked, data.status || '');
  const currentStatusColor = statusColors[badgeInfo.color as keyof typeof statusColors || 'blue'];
  
  const analysisReason = getAnalysisReason(data.status || '');
  const translatedConfidence = translateConfidence(data.geospatial_confidence_level || 'N/A');

  return (
    <Document title={`LAUDO_FORENSE_${data.propertyId}`}>
      <Page size="A4" style={styles.page}>
        
        {/* CABEÇALHO - Adicionado fixed para garantir consistência */}
        <View style={styles.header} fixed>
          <Image src="/logo-agrimarket.png" style={styles.logoAgri} />
          <View style={styles.headerInfo}>
            <Text style={styles.reportTitle}>Laudo de Auditoria Socioambiental Forense</Text>
            <Text style={styles.engineTag}>Powered by Caipora Sentinela</Text>
            <Text style={[styles.label, {marginTop: 2}]}>ID: {data.propertyId} | Protocolo: {data.propertyAlias || 'N/A'}</Text>
          </View>
          <Image src="/logo-caipora.jpg" style={styles.logoCaipora} />
        </View>

        {/* BANNER DE STATUS - wrap={false} para não separar o texto do fundo */}
        <View style={[styles.statusBanner, { backgroundColor: currentStatusColor }]} wrap={false}>
          <View>
            <Text style={styles.statusText}>{badgeInfo.label}</Text>
            <Text style={styles.confidenceText}>Confiança: {translatedConfidence}</Text>
          </View>
          <View style={{ textAlign: 'right' }}>
            <Text style={styles.statusText}>CAR: {translateStatus(data.car_status || '')}</Text>
            <Text style={styles.confidenceText}>Status Espacial: {data.car_status_spatial || 'Não Analisado'}</Text>
          </View>
        </View>

        {/* 01. IDENTIFICAÇÃO */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>01. Identificação, Perimetria e Topografia</Text>
          <View style={styles.grid}>
            <View style={styles.col4}><Text style={styles.label}>Tipo de Imóvel</Text><Text style={styles.value}>{data.property_identity_type}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Área CAR</Text><Text style={styles.value}>{data.area_ha?.toFixed(4)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Área Processada</Text><Text style={styles.value}>{data.area_geometria_ha?.toFixed(4)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Área Líquida</Text><Text style={styles.value}>{data.area_liquida_ha?.toFixed(4)} ha</Text></View>
            
            <View style={styles.col4}><Text style={styles.label}>Módulos Fiscais</Text><Text style={styles.value}>{data.fiscal_modules?.toFixed(2)} MF</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Relevo</Text><Text style={styles.value}>{reliefMap[data.relief_classification || ''] || 'Não Informado'}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Inclinação Máx</Text><Text style={styles.value}>{data.max_slope_degrees?.toFixed(2)}°</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Categoria</Text><Text style={styles.value}>{producerSizeMap[data.producer_size_category || ''] || 'Não Classificado'}</Text></View>
            
            <View style={styles.col2}><Text style={styles.label}>Município/UF</Text><Text style={styles.value}>{data.city} - {data.uf_origem}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Latitude</Text><Text style={styles.value}>{data.latitude?.toFixed(6)}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Longitude</Text><Text style={styles.value}>{data.longitude?.toFixed(6)}</Text></View>
          </View>
        </View>

        {/* 02. PARECER TÉCNICO */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>02. Parecer Técnico e Lógica de Conclusão</Text>
          <View style={{ backgroundColor: '#f1f5f9', padding: 6, borderLeft: '3pt solid #64748b' }}>
            <Text style={styles.label}>Veredito de Auditoria:</Text>
            <Text style={[styles.valueBold, { fontSize: 8 }]}>{analysisReason}</Text>
          </View>

          {!data.is_technically_blocked && (
            <View style={styles.successBox}>
              <Text style={styles.successText}>✓ Auditoria Concluída: Ausência de vetores de desmatamento ou irregularidades sociais detectados no período analisado.</Text>
            </View>
          )}

          <View style={data.is_technically_blocked ? styles.criticalBox : styles.highlightBox}>
            <Text style={[styles.label, data.is_technically_blocked ? {color: '#b91c1c'} : {}]}>Rastro da Perícia / Evidência Técnica:</Text>
            <Text style={{ fontSize: 7.2, marginTop: 3, fontWeight: 'bold', lineHeight: 1.2 }}>
              {data.risk_analysis?.technical_evidence || 'Nenhuma evidência técnica adicional registrada.'}
            </Text>
            <Text style={{ fontSize: 6.5, marginTop: 5, color: '#475569', borderTop: '0.5pt solid #cbd5e1', paddingTop: 4, fontStyle: 'italic' }}>
              Sumário Forense: {data.forensic_summary}
            </Text>
          </View>
        </View>

        {/* ANEXO CARTOGRÁFICO - wrap={false} impede que o mapa quebre ao meio */}
        {data.map_image_satellite && (
          <View style={styles.mapContainer} wrap={false}>
            <Image src={data.map_image_satellite} style={styles.mapImage} />
            <Text style={styles.mapCaption}>EVIDÊNCIA CARTOGRÁFICA: DELIMITAÇÃO E USO DO SOLO</Text>
          </View>
        )}

        {/* 03. CONFORMIDADE AMBIENTAL */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>03. Conformidade Ambiental e Marco Legal</Text>
          <View style={styles.grid}>
            <View style={styles.col4}><Text style={styles.label}>Bioma</Text><Text style={styles.value}>{data.environmental_score?.biome_name}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Marco Legal (Código)</Text><Text style={styles.value}>{formattedDate(data.environmental_score?.reference_forest_code_date)}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Status RL</Text><Text style={styles.value}>{rlStatusMap[data.environmental_score?.rl_status || ''] || data.environmental_score?.rl_status}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Déficit de RL</Text><Text style={[styles.value, {color: data.environmental_score?.rl_deficit_ha > 0 ? 'red' : 'green'}]}>{data.environmental_score?.rl_deficit_ha?.toFixed(2)} ha</Text></View>
          </View>
          
          <View style={[styles.grid, { marginTop: 4, backgroundColor: '#f8fafc', padding: 4 }]}>
            <View style={styles.col4}><Text style={styles.label}>APP Hídrica (Perícia)</Text><Text style={styles.value}>{data.environmental_score?.forensic_app_hidrica_ha?.toFixed(2)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>APP Declividade</Text><Text style={styles.value}>{data.environmental_score?.forensic_app_declividade_ha?.toFixed(2)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Área Pousio</Text><Text style={styles.value}>{data.environmental_score?.area_pousio_ha?.toFixed(2)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Restrição EUDR</Text><Text style={[styles.valueBold, {color: data.environmental_score?.is_eudr_restricted ? 'red' : 'green'}]}>{data.environmental_score?.is_eudr_restricted ? 'RESTRITO' : 'LIBERADO'}</Text></View>
          </View>
        </View>

        {/* 04. DESMATAMENTO - wrap={false} aqui evita que o título fique sozinho na página anterior */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>04. Histórico de Supressão de Vegetação Nativa</Text>
          <View style={styles.grid}>
            <View style={styles.col4}><Text style={styles.label}>MapBiomas</Text><Text style={styles.valueBold}>{data.deforestation_metrics?.mapbiomas_deforested_ha?.toFixed(4)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>EUDR (Pós-2020)</Text><Text style={styles.value}>{data.deforestation_metrics?.eudr_deforested_ha?.toFixed(4)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Data Detecção</Text><Text style={styles.value}>{formattedDate(data.deforestation_metrics?.mapbiomas_detection_date)}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Alertas Oficiais</Text><Text style={styles.value}>{data.deforestation_metrics?.official_alert_area_ha?.toFixed(4)} ha</Text></View>
          </View>
          
          <View style={styles.highlightBox}>
            <View style={styles.grid}>
              <View style={styles.col2}><Text style={styles.label}>Tipos de Vegetação Suprimida</Text><Text style={styles.value}>{translateDeforestationTypes(data.deforestation_metrics?.deforestation_types || '')}</Text></View>
              <View style={styles.col2}><Text style={styles.label}>IDs Alertas MapBiomas</Text><Text style={styles.value}>{data.deforestation_metrics?.mapbiomas_alert_ids || 'N/A'}</Text></View>
            </View>
            <View style={[styles.grid, {marginTop: 4}]}>
              <View style={styles.col2}><Text style={styles.label}>Evidência Satélite (Antes)</Text><Text style={styles.value}>{formattedDate(data.deforestation_metrics?.evidence_date_before)}</Text></View>
              <View style={styles.col2}><Text style={styles.label}>Evidência Satélite (Depois)</Text><Text style={styles.value}>{formattedDate(data.deforestation_metrics?.evidence_date_after)}</Text></View>
            </View>
          </View>
        </View>

        {/* 05. SOCIAL */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>05. Análise Social e Territórios Protegidos</Text>
          <View style={styles.tableHeader}>
            <Text style={[styles.tableCell, { flex: 2, fontWeight: 'bold' }]}>Tipo de Território</Text>
            <Text style={[styles.tableCell, { flex: 3, fontWeight: 'bold' }]}>Nome Identificado</Text>
            <Text style={[styles.tableCell, { flex: 1, fontWeight: 'bold' }]}>Área (ha)</Text>
            <Text style={[styles.tableCell, { flex: 1, fontWeight: 'bold' }]}>% Sobrep.</Text>
          </View>
          
          {[
            { label: 'Terra Indígena', name: data.social_score?.ti_name, ha: data.social_score?.forensic_ti_ha, pct: data.social_score?.ti_overlap_pct },
            { label: 'Unidade Conservação', name: data.social_score?.uc_name, ha: data.social_score?.forensic_uc_ha, pct: data.social_score?.uc_overlap_pct },
            { label: 'Assentamento', name: data.social_score?.settlement_name, ha: data.social_score?.forensic_settlement_ha, pct: data.social_score?.settlement_overlap_pct },
            { label: 'Quilombo', name: data.social_score?.quilombo_name, ha: data.social_score?.forensic_quilombo_ha, pct: 0 },
            { label: 'Comunidade Tradicional', name: data.social_score?.traditional_name, ha: data.social_score?.forensic_traditional_ha, pct: data.social_score?.traditional_overlap_pct },
          ].map((item, idx) => (item.ha > 0 || item.pct > 0) && (
            <View key={idx} style={styles.tableRow}>
              <Text style={[styles.tableCell, { flex: 2 }]}>{item.label}</Text>
              <Text style={[styles.tableCell, { flex: 3 }]}>{item.name || 'Identificado'}</Text>
              <Text style={[styles.tableCell, { flex: 1 }]}>{item.ha?.toFixed(2)}</Text>
              <Text style={[styles.tableCell, { flex: 1 }]}>{item.pct?.toFixed(2)}%</Text>
            </View>
          ))}

          <View style={[styles.grid, { marginTop: 6, padding: 4, backgroundColor: data.social_score?.slave_labor_overlap_ha > 0 ? '#fef2f2' : '#f8fafc' }]}>
            <View style={styles.col2}><Text style={styles.label}>Trabalho Escravo (Sobreposição)</Text><Text style={styles.valueBold}>{data.social_score?.slave_labor_overlap_ha > 0 ? 'SIM - DETECTADO' : 'NÃO CONSTA'}</Text></View>
            <View style={styles.col2}><Text style={styles.label}>Data de Inclusão na Lista Suja</Text><Text style={styles.value}>{formattedDate(data.social_score?.slave_labor_inclusion_date)}</Text></View>
          </View>
        </View>

        {/* 06. RISCO E EMBARGOS */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>06. Inteligência de Risco e Vetores Logísticos</Text>
          <View style={styles.grid}>
            <View style={styles.col4}><Text style={styles.label}>CMN 5081 (Bacen)</Text><Text style={[styles.valueBold, data.risk_analysis?.is_cmn_5081_sensitive ? {color: 'red'} : {color: 'green'}]}>{data.risk_analysis?.is_cmn_5081_sensitive ? 'IMPEDIMENTO' : 'LIBERADO'}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Embargo Ativo</Text><Text style={styles.valueBold}>{data.risk_analysis?.is_embargo_active ? 'SIM' : 'NÃO'}</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Área Embargada</Text><Text style={styles.value}>{data.risk_analysis?.embargo_area_ha?.toFixed(2)} ha</Text></View>
            <View style={styles.col4}><Text style={styles.label}>Risco Logístico</Text><Text style={[styles.valueBold, data.risk_analysis?.logistics_risk_score > 70 ? {color: 'red'} : {}]}>{logisticsRiskMap[data.risk_analysis?.logistics_risk_level || ''] || data.risk_analysis?.logistics_risk_level}</Text></View>
          </View>
          
          {data.risk_analysis?.is_embargo_active && (
            <View style={styles.warningBox}>
              <Text style={styles.label}>Processos de Embargo:</Text>
              <Text style={styles.value}>{data.risk_analysis?.embargo_processes || 'N/A'}</Text>
            </View>
          )}

          <View style={[styles.grid, { marginTop: 4, borderTop: '0.5pt solid #e2e8f0', paddingTop: 4 }]}>
            <View style={styles.col3}><Text style={styles.label}>Corpos d'água Artificiais</Text><Text style={styles.value}>{data.risk_analysis?.count_artificial_water_bodies} detectados</Text></View>
            <View style={styles.col3}><Text style={styles.label}>Barreira Física</Text><Text style={styles.value}>{data.risk_analysis?.has_physical_barrier ? 'SIM' : 'NÃO'}</Text></View>
            <View style={styles.col3}><Text style={styles.label}>Score de Adjacência</Text><Text style={styles.value}>{data.risk_analysis?.max_adjacency_score}</Text></View>
          </View>
        </View>

        {/* 07. EVIDÊNCIAS E PASSIVOS */}
        <View style={styles.section} wrap={false}>
          <Text style={styles.sectionTitle}>07. Checklist de Evidências e Passivos Financeiros</Text>
          <View style={styles.grid}>
            {data.risk_analysis?.evidence_admin_array?.map((ev, i) => <Text key={i} style={[styles.evidenceTag, {backgroundColor: '#e0f2fe'}]}>[ADM] {ev}</Text>)}
            {data.risk_analysis?.evidence_environmental_array?.map((ev, i) => <Text key={i} style={[styles.evidenceTag, {backgroundColor: '#dcfce7'}]}>[AMB] {ev}</Text>)}
            {data.risk_analysis?.evidence_social_array?.map((ev, i) => <Text key={i} style={[styles.evidenceTag, {backgroundColor: '#fee2e2'}]}>[SOC] {ev}</Text>)}
            {data.risk_analysis?.evidence_infrastructure_array?.map((ev, i) => <Text key={i} style={[styles.evidenceTag, {backgroundColor: '#fef9c3'}]}>[INF] {ev}</Text>)}
          </View>

          <View style={{ marginTop: 8, padding: 8, backgroundColor: '#0f172a', borderRadius: 4 }}>
            <View style={styles.gridNoWrap}>
              <View style={styles.col2}>
                <Text style={[styles.label, {color: '#94a3b8'}]}>TOTAL ESTIMADO DE PASSIVOS</Text>
                <Text style={{ color: '#ffffff', fontSize: 12, fontWeight: 'bold' }}>{formatLiability(data.financial_liabilities?.estimated_financial_liability_brl || 0)}</Text>
              </View>
              <View style={styles.col2}>
                <View style={styles.gridNoWrap}>
                  <View style={{width: '50%'}}><Text style={[styles.label, {color: '#94a3b8'}]}>Multas Desmatamento</Text><Text style={{color: '#fff', fontSize: 7}}>{formatLiability(data.financial_liabilities?.liability_deforestation_brl)}</Text></View>
                  <View style={{width: '50%'}}><Text style={[styles.label, {color: '#94a3b8'}]}>Recuperação RL/APP</Text><Text style={{color: '#fff', fontSize: 7}}>{formatLiability(data.financial_liabilities?.liability_rl_brl + data.financial_liabilities?.liability_app_brl)}</Text></View>
                </View>
                <View style={[styles.gridNoWrap, {marginTop: 4}]}>
                  <View style={{width: '50%'}}><Text style={[styles.label, {color: '#94a3b8'}]}>Passivo Social</Text><Text style={{color: '#fff', fontSize: 7}}>{formatLiability(data.financial_liabilities?.liability_social_brl)}</Text></View>
                  <View style={{width: '50%'}}><Text style={[styles.label, {color: '#94a3b8'}]}>Multas Embargo</Text><Text style={{color: '#fff', fontSize: 7}}>{formatLiability(data.financial_liabilities?.liability_embargo_brl)}</Text></View>
                </View>
              </View>
            </View>
          </View>
        </View>

        {/* RODAPÉ */}
        <View style={styles.footer} fixed>
          <Text style={styles.footerText}>
            Este laudo utiliza cruzamento de dados geoespaciais de fontes oficiais (CAR, SIGEF, INPE, IBAMA, MapBiomas, MTE). 
            Processado por Caipora Sentinela em: {formattedDate(data.processed_at)}.
          </Text>
          <Text style={[styles.footerText, { marginTop: 2, fontWeight: 'bold' }]}>
            AgriMarket Intel © {new Date().getFullYear()} | Autenticidade Garantida pelo ID {data.propertyId}
          </Text>
        </View>

      </Page>
    </Document>
  );
};

export default AuditReportTemplate;