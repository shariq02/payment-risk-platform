-- ============================================================================
-- Seed Reference Data
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/06_seed_reference_data.sql
-- Purpose: Inserts seed rows into reference dimension tables.
--          These are stable reference values that do not come from Olist CSVs.
--          Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
-- Run: Once during Phase 3 setup via ingestion/setup_warehouse.py
-- Safe to re-run: yes (ON CONFLICT DO NOTHING)
-- ============================================================================

-- ============================================================================
-- DIM_REGION
-- Brazil regions used in Olist data
-- ============================================================================
INSERT INTO mart.dim_region (region_code, region_name, is_high_risk_region)
VALUES
    ('SUDESTE',   'Sudeste',        false),
    ('SUL',       'Sul',            false),
    ('NORDESTE',  'Nordeste',       false),
    ('CENTRO',    'Centro-Oeste',   false),
    ('NORTE',     'Norte',          false)
ON CONFLICT (region_code) DO NOTHING;

-- ============================================================================
-- DIM_STATE
-- All Brazilian states present in Olist seller and customer data
-- ============================================================================
INSERT INTO mart.dim_state (region_sk, state_code, state_name)
VALUES
    -- Sudeste
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUDESTE'), 'SP', 'Sao Paulo'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUDESTE'), 'RJ', 'Rio de Janeiro'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUDESTE'), 'MG', 'Minas Gerais'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUDESTE'), 'ES', 'Espirito Santo'),
    -- Sul
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUL'), 'PR', 'Parana'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUL'), 'SC', 'Santa Catarina'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'SUL'), 'RS', 'Rio Grande do Sul'),
    -- Nordeste
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'BA', 'Bahia'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'CE', 'Ceara'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'PE', 'Pernambuco'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'MA', 'Maranhao'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'PB', 'Paraiba'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'RN', 'Rio Grande do Norte'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'PI', 'Piaui'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'SE', 'Sergipe'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORDESTE'), 'AL', 'Alagoas'),
    -- Centro-Oeste
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'CENTRO'), 'GO', 'Goias'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'CENTRO'), 'DF', 'Distrito Federal'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'CENTRO'), 'MT', 'Mato Grosso'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'CENTRO'), 'MS', 'Mato Grosso do Sul'),
    -- Norte
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'AM', 'Amazonas'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'PA', 'Para'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'RO', 'Rondonia'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'AC', 'Acre'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'RR', 'Roraima'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'AP', 'Amapa'),
    ((SELECT region_sk FROM mart.dim_region WHERE region_code = 'NORTE'), 'TO', 'Tocantins')
ON CONFLICT (state_code) DO NOTHING;

-- ============================================================================
-- DIM_CUSTOMER_RISK_TIER
-- Four tiers based on composite risk score ranges
-- Thresholds stored as dbt variables in dbt_project.yml
-- ============================================================================
INSERT INTO mart.dim_customer_risk_tier
    (tier_code, tier_name, score_lower_bound, score_upper_bound)
VALUES
    ('tier_1', 'Low Risk',      0.00, 0.25),
    ('tier_2', 'Medium Risk',   0.25, 0.50),
    ('tier_3', 'High Risk',     0.50, 0.75),
    ('tier_4', 'Critical Risk', 0.75, 1.00)
ON CONFLICT (tier_code) DO NOTHING;

-- ============================================================================
-- DIM_CUSTOMER_SEGMENT
-- Four segments based on customer payment behaviour
-- Assigned in dbt intermediate model int_customer_behaviour.sql
-- ============================================================================
INSERT INTO mart.dim_customer_segment (risk_tier_sk, segment_code, segment_name)
VALUES
    (
        (SELECT risk_tier_sk FROM mart.dim_customer_risk_tier WHERE tier_code = 'tier_1'),
        'new_customer',
        'New Customer'
    ),
    (
        (SELECT risk_tier_sk FROM mart.dim_customer_risk_tier WHERE tier_code = 'tier_1'),
        'returning_customer',
        'Returning Customer'
    ),
    (
        (SELECT risk_tier_sk FROM mart.dim_customer_risk_tier WHERE tier_code = 'tier_2'),
        'high_value_customer',
        'High Value Customer'
    ),
    (
        (SELECT risk_tier_sk FROM mart.dim_customer_risk_tier WHERE tier_code = 'tier_3'),
        'at_risk_customer',
        'At Risk Customer'
    )
ON CONFLICT (segment_code) DO NOTHING;

-- ============================================================================
-- DIM_SELLER_INDUSTRY
-- Broad industry groupings for Olist seller categories
-- ============================================================================
INSERT INTO mart.dim_seller_industry (industry_code, industry_name)
VALUES
    ('retail_general',    'General Retail'),
    ('electronics',       'Electronics and Technology'),
    ('fashion',           'Fashion and Accessories'),
    ('home_living',       'Home and Living'),
    ('health_beauty',     'Health and Beauty'),
    ('sports_leisure',    'Sports and Leisure'),
    ('food_drink',        'Food and Drink'),
    ('automotive',        'Automotive'),
    ('other',             'Other')
ON CONFLICT (industry_code) DO NOTHING;

-- ============================================================================
-- DIM_SELLER_CATEGORY
-- Seller categories mapped to industries
-- These are operational categories, not Olist product categories
-- ============================================================================
INSERT INTO mart.dim_seller_category (industry_sk, category_code, category_name)
VALUES
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'retail_general'),
        'marketplace_general', 'General Marketplace Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'electronics'),
        'electronics_seller', 'Electronics Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'fashion'),
        'fashion_seller', 'Fashion Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'home_living'),
        'home_seller', 'Home and Living Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'health_beauty'),
        'health_beauty_seller', 'Health and Beauty Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'sports_leisure'),
        'sports_seller', 'Sports and Leisure Seller'
    ),
    (
        (SELECT industry_sk FROM mart.dim_seller_industry WHERE industry_code = 'other'),
        'other_seller', 'Other Seller'
    )
ON CONFLICT (category_code) DO NOTHING;

-- ============================================================================
-- DIM_PRODUCT_DEPARTMENT
-- Olist has 73 product categories grouped into 9 departments
-- English names used throughout mart layer
-- ============================================================================
INSERT INTO mart.dim_product_department (department_code, department_name)
VALUES
    ('electronics',     'Electronics and Computers'),
    ('home_living',     'Home and Living'),
    ('fashion',         'Fashion and Accessories'),
    ('health_beauty',   'Health and Beauty'),
    ('sports_leisure',  'Sports and Leisure'),
    ('food_drink',      'Food and Drink'),
    ('baby_kids',       'Baby and Kids'),
    ('automotive',      'Automotive'),
    ('other',           'Other')
ON CONFLICT (department_code) DO NOTHING;

-- ============================================================================
-- DIM_PRODUCT_CATEGORY
-- All 73 Olist product categories mapped to departments
-- English names from product_category_name_translation.csv
-- 2 categories missing translation use Portuguese name as fallback:
--   pc_gamer, portateis_cozinha_e_preparadores_de_alimentos
-- ============================================================================
INSERT INTO mart.dim_product_category
    (department_sk, category_code, category_name_english, category_name_portuguese)
VALUES
    -- Electronics
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'computers_accessories', 'Computers Accessories', 'informatica_acessorios'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'electronics', 'Electronics', 'eletronicos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'tablets_printing_image', 'Tablets Printing Image', 'tablets_impressao_imagem'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'telephony', 'Telephony', 'telefonia'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'fixed_telephony', 'Fixed Telephony', 'telefonia_fixa'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'consoles_games', 'Consoles Games', 'consoles_games'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'pc_gamer', 'PC Gamer', 'pc_gamer'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'small_appliances', 'Small Appliances', 'eletroportateis'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'small_appliances_home_oven', 'Small Appliances Home Oven', 'pequenos_aparelhos_domesticos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'home_appliances', 'Home Appliances', 'eletrodomesticos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'electronics'),
     'home_appliances_2', 'Home Appliances 2', 'eletrodomesticos_2'),
    -- Home and Living
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'bed_bath_table', 'Bed Bath Table', 'cama_mesa_banho'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'furniture_decor', 'Furniture Decor', 'moveis_decoracao'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'housewares', 'Housewares', 'utilidades_domesticas'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'garden_tools', 'Garden Tools', 'ferramentas_jardim'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'furniture_living_room', 'Furniture Living Room', 'moveis_sala'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'furniture_bedroom', 'Furniture Bedroom', 'moveis_quarto'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'furniture_mattress_upholstery', 'Furniture Mattress Upholstery', 'moveis_colchao_e_estofado'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'furniture_office', 'Furniture Office', 'moveis_escritorio'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'home_comfort', 'Home Comfort', 'casa_conforto'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'home_comfort_2', 'Home Comfort 2', 'casa_conforto_2'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'home_construction', 'Home Construction', 'construcao_ferramentas_construcao'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'construction_tools_lights', 'Construction Tools Lights', 'construcao_ferramentas_iluminacao'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'construction_tools_safety', 'Construction Tools Safety', 'construcao_ferramentas_seguranca'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'construction_tools_tools', 'Construction Tools', 'construcao_ferramentas_ferramentas'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'kitchen_dining_laundry', 'Kitchen Dining Laundry', 'cozinha_jantar_lazer'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'portable_kitchen', 'Portable Kitchen', 'portateis_cozinha_e_preparadores_de_alimentos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'air_conditioning', 'Air Conditioning', 'climatizacao'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'home_living'),
     'flowers', 'Flowers', 'flores'),
    -- Fashion
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_bags_accessories', 'Fashion Bags Accessories', 'fashion_bolsas_e_acessorios'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_male_clothing', 'Fashion Male Clothing', 'fashion_roupa_masculina'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_female_clothing', 'Fashion Female Clothing', 'fashion_roupa_feminina'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_shoes', 'Fashion Shoes', 'fashion_calcados'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_sport', 'Fashion Sport', 'fashion_esporte'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'fashion_underwear_beach', 'Fashion Underwear Beach', 'fashion_roupa_infanto_juvenil'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'watches_gifts', 'Watches Gifts', 'relogios_presentes'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'luggage_accessories', 'Luggage Accessories', 'malas_acessorios'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'stationery', 'Stationery', 'papelaria'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'fashion'),
     'party_supplies', 'Party Supplies', 'artigos_de_festas'),
    -- Health and Beauty
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'health_beauty'),
     'health_beauty', 'Health Beauty', 'beleza_saude'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'health_beauty'),
     'perfumery', 'Perfumery', 'perfumaria'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'health_beauty'),
     'diapers_hygiene', 'Diapers Hygiene', 'fraldas_higiene'),
    -- Sports and Leisure
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'sports_leisure'),
     'sports_leisure', 'Sports Leisure', 'esporte_lazer'),
    -- Food and Drink
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'food_drink'),
     'food_drink', 'Food Drink', 'alimentos_bebidas'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'food_drink'),
     'food', 'Food', 'alimentos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'food_drink'),
     'drinks', 'Drinks', 'bebidas'),
    -- Baby and Kids
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'baby_kids'),
     'baby', 'Baby', 'bebes'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'baby_kids'),
     'toys', 'Toys', 'brinquedos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'baby_kids'),
     'kids_fashion', 'Kids Fashion', 'fashion_roupa_infanto_juvenil'),
    -- Automotive
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'automotive'),
     'auto', 'Auto', 'automotivo'),
    -- Other
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'cool_stuff', 'Cool Stuff', 'cool_stuff'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'arts_and_craftmanship', 'Arts and Craftmanship', 'artes_e_artesanato'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'art', 'Art', 'artes'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'books_general_interest', 'Books General Interest', 'livros_interesse_geral'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'books_technical', 'Books Technical', 'livros_tecnicos'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'books_imported', 'Books Imported', 'livros_importados'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'music', 'Music', 'musica'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'musical_instruments', 'Musical Instruments', 'instrumentos_musicais'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'dvds_blu_ray', 'DVDs Blu Ray', 'dvds_blu_ray'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'cds_dvds_musicals', 'CDs DVDs Musicals', 'cds_dvds_musicais'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'christmas_supplies', 'Christmas Supplies', 'artigos_de_natal'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'signaling_and_security', 'Signaling and Security', 'sinalizacao_e_seguranca'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'industry_commerce_business', 'Industry Commerce Business', 'industria_comercio_e_negocios'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'office_furniture', 'Office Furniture', 'moveis_escritorio'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'pet_shop', 'Pet Shop', 'pet_shop'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'agro_industry_commerce', 'Agro Industry Commerce', 'agro_industria_e_comercio'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'market_place', 'Market Place', 'market_place'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'unpublished', 'Unpublished', 'la_cuisine'),
    ((SELECT department_sk FROM mart.dim_product_department WHERE department_code = 'other'),
     'tablets_printing_image_2', 'Tablets Printing Image 2', 'tablets_impressao_imagem')
ON CONFLICT (category_code) DO NOTHING;

-- ============================================================================
-- DIM_PAYMENT_METHOD
-- All payment types found in Olist data including not_defined (3 rows)
-- ============================================================================
INSERT INTO mart.dim_payment_method
    (method_code, method_name, method_family, is_reversible, chargeback_eligible)
VALUES
    ('credit_card',  'Credit Card',   'card',           true,  true),
    ('debit_card',   'Debit Card',    'card',           false, true),
    ('boleto',       'Boleto',        'bank_slip',      false, false),
    ('voucher',      'Voucher',       'digital_wallet', false, false),
    ('not_defined',  'Not Defined',   'other',          false, false)
ON CONFLICT (method_code) DO NOTHING;
