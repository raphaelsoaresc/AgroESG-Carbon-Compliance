import { useState, useEffect } from 'react';
import { supabase } from '../lib/supabase';
import { AuditData, DEMO_IDS } from '../types';
import {
  carStatusMap,
  getAnalysisReason,
  formattedDate,
  determineStatusColor,
  translateDeforestationTypes,
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
    // 1. Checar sessão do Supabase
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setIsAdmin(!!session);
    });

    // 2. Ouvir mudanças na auth
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
      if (!prefId && result.init_point) {
        const url = new URL(result.init_point);
        prefId = url.searchParams.get('pref_id');
      }
      if (prefId) {
        setPreferenceId(prefId);
        setShowPayment(true);
      }
    } catch (error) {
      console.error("Erro de Rede:", error);
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
      if (!response.ok) return;
      const record = await response.json();
      const mainGeometry = record.geom_car_total || record.geometry;

      if (!isDemo && !freshSession) {
        const newCount = searchCount + 1;
        setSearchCount(newCount);
        localStorage.setItem('caipora_search_count', newCount.toString());
        if (newCount >= 3) setLimitReached(true);
      }

      const risk = record.risk_analysis;
      
      const defo = record.deforestation_metrics;

      // AJUSTE A: Limpeza da String de Riscos (UC/TI)
      const rawRisks = risk?.internal_risks_found || "";
      const cleanedRisks = rawRisks.replace(/MAPBIOMAS|DECLIVIDADE|EMBARGO|RESTRIÇÃO EXPORTAÇÃO|RESTRIÇÃO|EUDR|\(EUDR\)|[|]/gi, '').trim().replace(/\s+/g, ' ').replace(/^,\s*|,\s*$/g, '');

      const social = record.social_score;

      const evidenceList = [
        `🌳 Bioma: ${record.environmental_score?.biome_name}.`,
        `🚜 Classificação Fundiária: ${record.producer_size_category}.`,
        `🆔 Situação Cadastral: Imóvel ${record.car_status}.`,
        `📊 Nível de Confiança Geoespacial: ${translateConfidence(record.geospatial_confidence_level)}.`,

        // Relevo e Declividade
        record.relief_classification ? 
          (['MONTANHOSO', 'ESCARPADO'].includes(record.relief_classification.toUpperCase())
            ? `⚠️ Relevo Médio: ${record.relief_classification} (Atenção: Alta Declividade).`
            : `🏔️ Relevo Médio: ${record.relief_classification}.`)
          : null,

        record.max_slope_degrees !== undefined && record.max_slope_degrees !== null ?
          (record.max_slope_degrees >= 45 
            ? `🚨 Declividade Máxima (Crítica): ${record.max_slope_degrees}° (Acima do limite legal de 45°).`
            : `📐 Declividade Máxima: ${record.max_slope_degrees}°.`)
          : null,

        record.environmental_score?.is_eudr_restricted ? `🇪🇺 Bloqueio EUDR: Imóvel restrito para exportação à União Europeia (Desmatamento pós-2020).` : null,
        defo?.mapbiomas_deforested_ha > 0 ? 
          `🪓 Supressão Vegetal (MapBiomas): ${defo.mapbiomas_deforested_ha.toFixed(2)} ha. Causa: ${translateDeforestationTypes(defo.deforestation_types)}. Detectado em: ${formattedDate(defo.mapbiomas_detection_date)}.` 
          : null,
        
        // Alertas Críticos de Sobreposição (Forensic) - Só aparecem se > 0
        social?.forensic_ti_ha > 0 ? `🚨 Alerta: Sobreposição em Terra Indígena (${social.forensic_ti_ha.toFixed(2)} ha).` : null,
        social?.forensic_quilombo_ha > 0 ? `🚨 Alerta: Sobreposição em Quilombo (${social.forensic_quilombo_ha.toFixed(2)} ha).` : null,
        social?.forensic_uc_ha > 0 ? `🚨 Alerta: Sobreposição em Unidade de Conservação (${social.forensic_uc_ha.toFixed(2)} ha).` : null,
        social?.forensic_settlement_ha > 0 ? `🚨 Alerta: Sobreposição em Assentamento (${social.forensic_settlement_ha.toFixed(2)} ha).` : null,

        record.environmental_score?.rl_deficit_ha > 0 ? `📜 Passivo Ambiental: Déficit de ${record.environmental_score.rl_deficit_ha.toFixed(2)} ha de Reserva Legal (RL).` : null,
        record.final_eligibility_status === 'WARNING - RISK BY ADJACENCY' 
          ? `🏠 Risco por Adjacência: ${risk?.adjacency_details}.` 
          : (record.final_eligibility_status?.startsWith('ELIGIBLE') 
              ? "🛡️ Perímetro Seguro: Não foram detectados riscos críticos em imóveis confrontantes" 
              : null),
      ].filter(Boolean) as string[];
      setData({
        status: record.final_eligibility_status,
        analysisReason: getAnalysisReason(record.final_eligibility_status, record.geospatial_confidence_level || 'N/A'),
        carStatus: record.car_status || 'ATIVO',
        processedAt: formattedDate(record.processed_at),
        analyzedAt: formattedDate(record.analyzed_at || record.processed_at),
        mapbiomasUrl: defo?.official_reports_urls, 
        relief: record.relief_classification || 'Nível',
        carNumber: record.property_id,
        uf: record.uf_origem || 'N/A',
        city: record.city || 'Não Informada',
        area: record.property_area_ha || 0,
        liabilityTotal: formatCurrency(record.financial_liabilities?.estimated_financial_liability_brl || 0),
        liabilityAmbientalTotal: formatCurrency(
          (record.financial_liabilities?.liability_deforestation_brl || 0) +
          (record.financial_liabilities?.liability_rl_brl || 0) +
          (record.financial_liabilities?.liability_app_brl || 0)
        ),
        liabilityEmbargo: formatCurrency(record.financial_liabilities?.liability_embargo_brl || 0),
        liabilityProtected: formatCurrency(record.financial_liabilities?.liability_protected_areas_brl || 0),
        liabilitySocial: formatCurrency(record.financial_liabilities?.liability_social_brl || 0),
        liabilityDeforestation: formatCurrency(record.financial_liabilities?.liability_deforestation_brl || 0),
        liabilityRL: formatCurrency(record.financial_liabilities?.liability_rl_brl || 0),
        liabilityAPP: formatCurrency(record.financial_liabilities?.liability_app_brl || 0),
        evidenceList,
        metrics: `Bioma: ${record.environmental_score?.biome_name || 'N/A'} | RL: ${record.environmental_score?.rl_status || 'N/A'}`,
        color: determineStatusColor(record.final_eligibility_status, record.geospatial_confidence_level || 'N/A'),
        mapCenterCoords: getCenterPoint(mainGeometry),
        geometry: typeof mainGeometry === 'string' ? JSON.parse(mainGeometry) : mainGeometry,
        geom_embargos: typeof record.geom_embargos === 'string' ? JSON.parse(record.geom_embargos) : record.geom_embargos,
        geom_desmatamento: typeof record.geom_desmatamento === 'string' ? JSON.parse(record.geom_desmatamento) : record.geom_desmatamento,
        geom_areas_protegidas: typeof record.geom_areas_protegidas === 'string' ? JSON.parse(record.geom_areas_protegidas) : record.geom_areas_protegidas,
        geom_conflito_app: typeof record.geom_conflito_app === 'string' ? JSON.parse(record.geom_conflito_app) : record.geom_conflito_app,
        geom_assentamentos: record.geom_assentamentos,
        isCensored: !!freshSession ? false : !isDemo,
        confidenceLevel: translateConfidence(record.geospatial_confidence_level),
        isTechnicallyBlocked: record.is_technically_blocked,
        producerSizeCategory: record.producer_size_category,
        internalRisks: record.risk_analysis?.internal_risks_found,
        historicalWarnings: record.environmental_score?.historical_warnings,
        embargoProcesses: risk?.embargo_processes,
        embargoOffenders: risk?.embargo_offenders,
      });
    } catch (error) {
      console.error("Erro na busca:", error);
    } finally {
      setLoading(false);
    }
  };

  const logout = async () => {
    await supabase.auth.signOut();
    window.location.reload();
  };

  return { carId, setCarId, data, loading, searchCount, showPayment, preferenceId, limitReached, handleUnlockReport, performSearch, isAdmin, logout };
}