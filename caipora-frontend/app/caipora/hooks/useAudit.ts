import { useState, useEffect } from 'react';
import { supabase } from '../lib/supabase';
import { AuditData, DEMO_IDS } from '../types';
import {
  getAnalysisReason,
  formattedDate,
  determineStatusColor,
  translateConfidence
} from '../lib/audit-utils';
import { getCenterPoint, formatCurrency } from '../utils';

export function useAudit() {
  const [session, setSession] = useState<any>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [carId, setCarId] = useState('');
  const [data, setData] = useState<AuditData | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchCount, setSearchCount] = useState(0);
  const [showPayment, setShowPayment] = useState(false);
  const [preferenceId, setPreferenceId] = useState<string | null>(null);
  const [limitReached, setLimitReached] = useState(false);

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setIsAdmin(!!session);
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
      setIsAdmin(!!session);
    });
    return () => subscription.unsubscribe();
  }, []);

  useEffect(() => {
    const savedCount = localStorage.getItem('caipora_search_count');
    if (savedCount && !isAdmin) {
      const count = parseInt(savedCount);
      setSearchCount(count);
      if (count >= 3) setLimitReached(true);
    }
  }, [isAdmin]);

  const handleUnlockReport = async () => {
    setLoading(true);
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "").replace(/\/$/, "");
      const apiKey = process.env.NEXT_PUBLIC_API_KEY;
      const response = await fetch(`${apiUrl}/payments/create-preference`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-Key": apiKey || "" },
        body: JSON.stringify({ car_id: carId, email: "compliance@agrimarketintel.com" })
      });
      const result = await response.json();
      let prefId = result.id || result.preference_id;
      if (prefId) {
        setPreferenceId(prefId);
        setShowPayment(true);
      }
    } catch (error) {
      console.error("Erro ao criar preferência de pagamento:", error);
    } finally {
      setLoading(false);
    }
  };

  const performSearch = async (idToSearch: string) => {
    if (!idToSearch) return;

    const { data: { session: freshSession } } = await supabase.auth.getSession();
    const isDemo = DEMO_IDS.includes(idToSearch);

    if (!isDemo && !freshSession && searchCount >= 3) {
      setLimitReached(true);
      setCarId(idToSearch);
      setData(null);
      return;
    }

    setLoading(true);
    setCarId(idToSearch);
    setShowPayment(false);
    setPreferenceId(null);
    setLimitReached(false);

    const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    const apiKey = process.env.NEXT_PUBLIC_API_KEY;

    const headers: any = {
      "X-API-Key": apiKey || "",
      "Content-Type": "application/json"
    };

    if (freshSession?.access_token) {
      headers["Authorization"] = `Bearer ${freshSession.access_token}`;
    }

    try {
      const response = await fetch(`${apiUrl}/compliance/car/${idToSearch}`, { headers });
      if (!response.ok) throw new Error("Falha na requisição");
      
      const record = await response.json();
      
      // Helper para garantir que geometrias em string sejam convertidas em objetos
      const parseGeom = (g: any) => {
        if (!g) return null;
        return typeof g === 'string' ? JSON.parse(g) : g;
      };

      const mainGeometry = parseGeom(record.geom_car_total || record.geometry);

      if (!isDemo && !freshSession) {
        const newCount = searchCount + 1;
        setSearchCount(newCount);
        localStorage.setItem('caipora_search_count', newCount.toString());
        if (newCount >= 3) setLimitReached(true);
      }

      // MAPEAMENTO INTEGRAL E EXAUSTIVO DA API PARA O ESTADO
      setData({
        // 1. Identificação Básica
        propertyId: record.property_id,
        propertyAlias: record.property_alias,
        carNumber: record.property_id,
        area: record.property_area_ha,
        areaHa: record.area_ha,
        propertyAreaHa: record.property_area_ha,
        city: record.city,
        uf: record.uf_origem,
        carStatus: record.car_status || 'ATIVO',

        // 2. Localização e Perícia Física
        latitude: record.latitude,
        longitude: record.longitude,
        carBbox: record.car_bbox,
        mapCenterCoords: [record.latitude, record.longitude],
        maxSlopeDegrees: record.max_slope_degrees,
        relief: record.relief_classification || 'Nível',

        // 3. Status de Elegibilidade e Confiança
        status: record.final_eligibility_status,
        statusDetailed: record.final_eligibility_status_detailed,
        finalEligibilityStatus: record.final_eligibility_status,
        finalEligibilityStatusDetailed: record.final_eligibility_status_detailed,
        isTechnicallyBlocked: record.is_technically_blocked,
        dataReliabilityIndex: record.data_reliability_index || 0,
        confidenceLevel: translateConfidence(record.geospatial_confidence_level),
        geospatialConfidenceLevel: record.geospatial_confidence_level,
        forensicSummary: record.forensic_summary,
        analysisReason: record.forensic_summary || getAnalysisReason(record.final_eligibility_status, record.geospatial_confidence_level),
        processedAt: formattedDate(record.processed_at),
        analyzedAt: formattedDate(record.analyzed_at || record.processed_at),

        // 4. Identidades e Categorias de Produtor
        isSettlementIdentity: record.is_settlement_identity,
        isTraditionalIdentity: record.is_traditional_identity,
        isQuilomboIdentity: record.is_quilombo_identity,
        isTiIdentity: record.is_ti_identity,
        isUcIdentity: record.is_uc_identity,
        isSmallHolder: record.is_small_holder,
        producerSizeCategory: record.producer_size_category || 'N/A',

        // 5. Objetos Brutos da API (Garantia de redundância)
        financialLiabilities: record.financial_liabilities,
        environmentalScore: record.environmental_score,
        deforestationMetrics: record.deforestation_metrics,
        socialScore: record.social_score,
        riskAnalysis: record.risk_analysis,

        // 6. Passivos Financeiros (Formatados para UI)
        liabilityTotal: formatCurrency(record.financial_liabilities?.estimated_financial_liability_brl || 0),
        liabilityAmbientalTotal: formatCurrency(
          (record.financial_liabilities?.liability_deforestation_brl || 0) + 
          (record.financial_liabilities?.liability_rl_brl || 0) +
          (record.financial_liabilities?.liability_app_brl || 0)
        ),
        liabilityEmbargo: formatCurrency(record.financial_liabilities?.liability_embargo_brl || 0),
        liabilityProtected: formatCurrency(record.financial_liabilities?.liability_protected_areas_brl || 0),
        liabilitySocial: formatCurrency(record.financial_liabilities?.liability_social_brl || 0),
        liabilityDeforestation: record.deforestation_metrics?.mapbiomas_deforested_ha 
          ? `${record.deforestation_metrics.mapbiomas_deforested_ha.toFixed(2)} ha` 
          : "0.00 ha",
        liabilityRL: formatCurrency(record.financial_liabilities?.liability_rl_brl || 0),
        liabilityAPP: formatCurrency(record.financial_liabilities?.liability_app_brl || 0),

        // 7. Ambiental e Risco (Campos extraídos)
        biomeName: record.environmental_score?.biome_name,
        rlStatus: record.environmental_score?.rl_status,
        isLiabilityUncertain: record.environmental_score?.is_liability_uncertain || false,
        historicalWarnings: record.environmental_score?.historical_warnings,
        maxAdjacencyScore: record.risk_analysis?.max_adjacency_score || 0,
        adjacentRoads: record.risk_analysis?.adjacent_roads,
        cityDataSourceOrigin: record.risk_analysis?.city_data_source_origin,
        internalRisks: record.risk_analysis?.internal_risks_found,
        embargoProcesses: record.risk_analysis?.embargo_processes,
        embargoOffenders: record.risk_analysis?.embargo_offenders,
        mapbiomasUrl: record.deforestation_metrics?.official_reports_urls,
        mapbiomasAlertIds: record.deforestation_metrics?.mapbiomas_alert_ids,
        liabilityDeforestationHa: record.deforestation_metrics?.mapbiomas_deforested_ha,

        // 8. Gavetas de Evidências (Perícia)
        evidenceAdmin: record.risk_analysis?.evidence_admin,
        evidenceSocial: record.risk_analysis?.evidence_social,
        evidenceEnvironmental: record.risk_analysis?.evidence_environmental,
        evidenceInfrastructure: record.risk_analysis?.evidence_infrastructure,
        evidenceList: [],

        // 9. Geometrias (GeoJSON para o Mapa)
        geometry: mainGeometry,
        geom_car_total: mainGeometry,
        geom_embargos: parseGeom(record.geom_embargos || record.geom_embargo),
        geom_desmatamento: parseGeom(record.geom_desmatamento),
        geom_eudr: parseGeom(record.geom_eudr),
        geom_areas_protegidas: parseGeom(record.geom_areas_protegidas),
        geom_conflito_app: parseGeom(record.geom_conflito_app),
        geom_assentamentos: parseGeom(record.geom_assentamentos),
        geom_adjacencia_risco: parseGeom(record.geom_adjacencia_risco),

        // 10. UI Helpers
        color: determineStatusColor(record.final_eligibility_status, record.geospatial_confidence_level),
        metrics: `Bioma: ${record.environmental_score?.biome_name || 'N/A'} | Confiança: ${record.geospatial_confidence_level}`,
        isCensored: !!freshSession ? false : !isDemo,
      });
    } catch (error) {
      console.error("Erro na busca de compliance:", error);
    } finally {
      setLoading(false);
    }
  };

  const logout = async () => {
    await supabase.auth.signOut();
    window.location.reload();
  };

  return { 
    carId, 
    setCarId, 
    data, 
    loading, 
    searchCount, 
    showPayment, 
    preferenceId, 
    limitReached, 
    handleUnlockReport, 
    performSearch, 
    isAdmin, 
    logout 
  };
}