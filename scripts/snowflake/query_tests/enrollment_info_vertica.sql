
select year(enrollment.created) enrollment_year,
  case when prod_user.user_last_location_country_code = 'BS' then 1 when prod_user.user_last_location_country_code = 'TZ' then 2 when prod_user.user_last_location_country_code = 'MX' then 3 when prod_user.user_last_location_country_code = 'HN' then 4 when prod_user.user_last_location_country_code = 'PG' then 5 when prod_user.user_last_location_country_code = 'PW' then 6 when prod_user.user_last_location_country_code = 'MU' then 7 when prod_user.user_last_location_country_code = 'MS' then 8 when prod_user.user_last_location_country_code = 'LU' then 9 when prod_user.user_last_location_country_code = 'GU' then 10 when prod_user.user_last_location_country_code = 'NI' then 11 when prod_user.user_last_location_country_code = 'PH' then 12 when prod_user.user_last_location_country_code = 'BB' then 13 when prod_user.user_last_location_country_code = 'NP' then 14 when prod_user.user_last_location_country_code = 'SD' then 15 when prod_user.user_last_location_country_code = 'SG' then 16 when prod_user.user_last_location_country_code = 'SE' then 17 when prod_user.user_last_location_country_code = 'KP' then 18 when prod_user.user_last_location_country_code = 'LR' then 19 when prod_user.user_last_location_country_code = 'GW' then 20 when prod_user.user_last_location_country_code = 'MH' then 21 when prod_user.user_last_location_country_code = 'KW' then 22 when prod_user.user_last_location_country_code = 'BF' then 23 when prod_user.user_last_location_country_code = 'AN' then 24 when prod_user.user_last_location_country_code = 'ZW' then 25 when prod_user.user_last_location_country_code = 'BZ' then 26 when prod_user.user_last_location_country_code = 'MP' then 27 when prod_user.user_last_location_country_code = 'CD' then 28 when prod_user.user_last_location_country_code = 'TM' then 29 when prod_user.user_last_location_country_code = 'PF' then 30 when prod_user.user_last_location_country_code = 'PK' then 31 when prod_user.user_last_location_country_code = 'WF' then 32 when prod_user.user_last_location_country_code = 'HU' then 33 when prod_user.user_last_location_country_code = 'BH' then 34 when prod_user.user_last_location_country_code = 'BD' then 35 when prod_user.user_last_location_country_code = 'MM' then 36 when prod_user.user_last_location_country_code = 'TN' then 37 when prod_user.user_last_location_country_code = 'IQ' then 38 when prod_user.user_last_location_country_code = 'MW' then 39 when prod_user.user_last_location_country_code = 'FI' then 40 when prod_user.user_last_location_country_code = 'BW' then 41 when prod_user.user_last_location_country_code = 'BG' then 42 when prod_user.user_last_location_country_code = 'OM' then 43 when prod_user.user_last_location_country_code = 'KZ' then 44 when prod_user.user_last_location_country_code = 'LC' then 45 when prod_user.user_last_location_country_code = 'IO' then 46 when prod_user.user_last_location_country_code = 'GA' then 47 when prod_user.user_last_location_country_code = 'KI' then 48 when prod_user.user_last_location_country_code = 'ID' then 49 when prod_user.user_last_location_country_code = 'FR' then 50 when prod_user.user_last_location_country_code = 'SI' then 51 when prod_user.user_last_location_country_code = 'AQ' then 52 when prod_user.user_last_location_country_code = 'CZ' then 53 when prod_user.user_last_location_country_code = 'CF' then 54 when prod_user.user_last_location_country_code = 'ZM' then 55 when prod_user.user_last_location_country_code = 'RS' then 56 when prod_user.user_last_location_country_code = 'CK' then 57 when prod_user.user_last_location_country_code = 'ER' then 58 when prod_user.user_last_location_country_code = 'RU' then 59 when prod_user.user_last_location_country_code = 'AX' then 60 when prod_user.user_last_location_country_code = 'GB' then 61 when prod_user.user_last_location_country_code = 'CG' then 62 when prod_user.user_last_location_country_code = 'TK' then 63 when prod_user.user_last_location_country_code = 'NG' then 64 when prod_user.user_last_location_country_code = 'KG' then 65 when prod_user.user_last_location_country_code = 'MG' then 66 when prod_user.user_last_location_country_code = 'AG' then 67 when prod_user.user_last_location_country_code = 'CN' then 68 when prod_user.user_last_location_country_code = 'NU' then 69 when prod_user.user_last_location_country_code = 'KE' then 70 when prod_user.user_last_location_country_code = 'MA' then 71 when prod_user.user_last_location_country_code = 'SZ' then 72 when prod_user.user_last_location_country_code = 'GR' then 73 when prod_user.user_last_location_country_code = 'CL' then 74 when prod_user.user_last_location_country_code = 'CI' then 75 when prod_user.user_last_location_country_code = 'TH' then 76 when prod_user.user_last_location_country_code = 'SS' then 77 when prod_user.user_last_location_country_code = 'KY' then 78 when prod_user.user_last_location_country_code = 'TL' then 79 when prod_user.user_last_location_country_code = 'AD' then 80 when prod_user.user_last_location_country_code = 'BT' then 81 when prod_user.user_last_location_country_code = 'BI' then 82 when prod_user.user_last_location_country_code = 'BE' then 83 when prod_user.user_last_location_country_code = 'TG' then 84 when prod_user.user_last_location_country_code = 'TO' then 85 when prod_user.user_last_location_country_code = 'BJ' then 86 when prod_user.user_last_location_country_code = 'DE' then 87 when prod_user.user_last_location_country_code = 'CR' then 88 when prod_user.user_last_location_country_code = 'SA' then 89 when prod_user.user_last_location_country_code = 'SO' then 90 when prod_user.user_last_location_country_code = 'ML' then 91 when prod_user.user_last_location_country_code = 'BR' then 92 when prod_user.user_last_location_country_code = 'UY' then 93 when prod_user.user_last_location_country_code = 'VN' then 94 when prod_user.user_last_location_country_code = 'IN' then 95 when prod_user.user_last_location_country_code = 'TD' then 96 when prod_user.user_last_location_country_code = 'LY' then 97 when prod_user.user_last_location_country_code = 'GN' then 98 when prod_user.user_last_location_country_code = 'ET' then 99 when prod_user.user_last_location_country_code = 'KH' then 100 when prod_user.user_last_location_country_code = 'HK' then 101 when prod_user.user_last_location_country_code = 'VE' then 102 when prod_user.user_last_location_country_code = 'MR' then 103 when prod_user.user_last_location_country_code = 'CY' then 104 when prod_user.user_last_location_country_code = 'AS' then 105 when prod_user.user_last_location_country_code = 'AE' then 106 when prod_user.user_last_location_country_code = 'KR' then 107 when prod_user.user_last_location_country_code = 'NC' then 108 when prod_user.user_last_location_country_code = 'GE' then 109 when prod_user.user_last_location_country_code = 'SH' then 110 when prod_user.user_last_location_country_code = 'AT' then 111 when prod_user.user_last_location_country_code = 'MD' then 112 when prod_user.user_last_location_country_code = 'AO' then 113 when prod_user.user_last_location_country_code = 'IR' then 114 when prod_user.user_last_location_country_code = 'US' then 115 when prod_user.user_last_location_country_code = 'RW' then 116 when prod_user.user_last_location_country_code = 'MK' then 117 when prod_user.user_last_location_country_code = 'FJ' then 118 when prod_user.user_last_location_country_code = 'TR' then 119 when prod_user.user_last_location_country_code = 'MZ' then 120 when prod_user.user_last_location_country_code = 'DJ' then 121 when prod_user.user_last_location_country_code = 'GH' then 122 when prod_user.user_last_location_country_code = 'PL' then 123 when prod_user.user_last_location_country_code = 'SB' then 124 when prod_user.user_last_location_country_code = 'GL' then 125 when prod_user.user_last_location_country_code = 'KM' then 126 when prod_user.user_last_location_country_code = 'LA' then 127 when prod_user.user_last_location_country_code = 'BA' then 128 when prod_user.user_last_location_country_code = 'AM' then 129 when prod_user.user_last_location_country_code = 'WS' then 130 when prod_user.user_last_location_country_code = 'TW' then 131 when prod_user.user_last_location_country_code = 'UZ' then 132 when prod_user.user_last_location_country_code = 'SV' then 133 when prod_user.user_last_location_country_code = 'CU' then 134 when prod_user.user_last_location_country_code = 'BM' then 135 when prod_user.user_last_location_country_code = 'TT' then 136 when prod_user.user_last_location_country_code = 'DZ' then 137 when prod_user.user_last_location_country_code = 'AF' then 138 when prod_user.user_last_location_country_code = 'PS' then 139 when prod_user.user_last_location_country_code = 'AZ' then 140 when prod_user.user_last_location_country_code = 'BO' then 141 when prod_user.user_last_location_country_code = 'RE' then 142 when prod_user.user_last_location_country_code = 'IM' then 143 when prod_user.user_last_location_country_code = 'EC' then 144 when prod_user.user_last_location_country_code = 'TC' then 145 when prod_user.user_last_location_country_code = 'AW' then 146 when prod_user.user_last_location_country_code = 'PM' then 147 when prod_user.user_last_location_country_code = 'QA' then 148 when prod_user.user_last_location_country_code = 'SY' then 149 when prod_user.user_last_location_country_code = 'JO' then 150 when prod_user.user_last_location_country_code = 'NR' then 151 when prod_user.user_last_location_country_code = 'LK' then 152 when prod_user.user_last_location_country_code = 'MY' then 153 when prod_user.user_last_location_country_code = 'GP' then 154 when prod_user.user_last_location_country_code = 'HT' then 155 when prod_user.user_last_location_country_code = 'AI' then 156 when prod_user.user_last_location_country_code = 'GQ' then 157 when prod_user.user_last_location_country_code = 'MO' then 158 when prod_user.user_last_location_country_code = 'PA' then 159 when prod_user.user_last_location_country_code = 'VU' then 160 when prod_user.user_last_location_country_code = 'TV' then 161 when prod_user.user_last_location_country_code = 'ES' then 162 when prod_user.user_last_location_country_code = 'VG' then 163 when prod_user.user_last_location_country_code = 'VC' then 164 when prod_user.user_last_location_country_code = 'GT' then 165 when prod_user.user_last_location_country_code = 'CM' then 166 when prod_user.user_last_location_country_code = 'MF' then 167 when prod_user.user_last_location_country_code = 'LV' then 168 when prod_user.user_last_location_country_code = 'LS' then 169 when prod_user.user_last_location_country_code = 'FX' then 170 when prod_user.user_last_location_country_code = 'KN' then 171 when prod_user.user_last_location_country_code = 'NO' then 172 when prod_user.user_last_location_country_code = 'LT' then 173 when prod_user.user_last_location_country_code = 'JM' then 174 when prod_user.user_last_location_country_code = 'DM' then 175 when prod_user.user_last_location_country_code = 'GD' then 176 when prod_user.user_last_location_country_code = 'PR' then 177 when prod_user.user_last_location_country_code = 'SK' then 178 when prod_user.user_last_location_country_code = 'GI' then 179 when prod_user.user_last_location_country_code = 'IL' then 180 when prod_user.user_last_location_country_code = 'BQ' then 181 when prod_user.user_last_location_country_code = 'NL' then 182 when prod_user.user_last_location_country_code = 'FO' then 183 when prod_user.user_last_location_country_code = 'NF' then 184 when prod_user.user_last_location_country_code = 'AL' then 185 when prod_user.user_last_location_country_code = 'CH' then 186 when prod_user.user_last_location_country_code = 'SC' then 187 when prod_user.user_last_location_country_code = 'SL' then 188 when prod_user.user_last_location_country_code = 'UG' then 189 when prod_user.user_last_location_country_code = 'SR' then 190 when prod_user.user_last_location_country_code = 'LI' then 191 when prod_user.user_last_location_country_code = 'RO' then 192 when prod_user.user_last_location_country_code = 'BN' then 193 when prod_user.user_last_location_country_code = 'SM' then 194 when prod_user.user_last_location_country_code = 'AU' then 195 when prod_user.user_last_location_country_code = 'IS' then 196 when prod_user.user_last_location_country_code = 'IT' then 197 when prod_user.user_last_location_country_code = 'JP' then 198 when prod_user.user_last_location_country_code = 'FK' then 199 when prod_user.user_last_location_country_code = 'ME' then 200 when prod_user.user_last_location_country_code = 'SN' then 201 when prod_user.user_last_location_country_code = 'MT' then 202 when prod_user.user_last_location_country_code = 'YE' then 203 when prod_user.user_last_location_country_code = 'MN' then 204 when prod_user.user_last_location_country_code = 'VI' then 205 when prod_user.user_last_location_country_code = 'CA' then 206 when prod_user.user_last_location_country_code = 'NE' then 207 when prod_user.user_last_location_country_code = 'NA' then 208 when prod_user.user_last_location_country_code = 'VA' then 209 when prod_user.user_last_location_country_code = 'MV' then 210 when prod_user.user_last_location_country_code = 'UM' then 211 when prod_user.user_last_location_country_code = 'PT' then 212 when prod_user.user_last_location_country_code = 'YT' then 213 when prod_user.user_last_location_country_code = 'NZ' then 214 when prod_user.user_last_location_country_code = 'GY' then 215 when prod_user.user_last_location_country_code = 'TJ' then 216 when prod_user.user_last_location_country_code = 'HR' then 217 when prod_user.user_last_location_country_code = 'UA' then 218 when prod_user.user_last_location_country_code = 'DO' then 219 when prod_user.user_last_location_country_code = 'BY' then 220 when prod_user.user_last_location_country_code = 'FM' then 221 when prod_user.user_last_location_country_code = 'AR' then 222 when prod_user.user_last_location_country_code = 'IE' then 223 when prod_user.user_last_location_country_code = 'MC' then 224 when prod_user.user_last_location_country_code = 'GG' then 225 when prod_user.user_last_location_country_code = 'CO' then 226 when prod_user.user_last_location_country_code = 'PE' then 227 when prod_user.user_last_location_country_code = 'JE' then 228 when prod_user.user_last_location_country_code = 'DK' then 229 when prod_user.user_last_location_country_code = 'LB' then 230 when prod_user.user_last_location_country_code = 'ST' then 231 when prod_user.user_last_location_country_code = 'GF' then 232 when prod_user.user_last_location_country_code = 'PY' then 233 when prod_user.user_last_location_country_code = 'EG' then 234 when prod_user.user_last_location_country_code = 'MQ' then 235 when prod_user.user_last_location_country_code = 'GM' then 236 when prod_user.user_last_location_country_code = 'ZA' then 237 when prod_user.user_last_location_country_code = 'EE' then 238 when prod_user.user_last_location_country_code = 'CV' then 239 else null end country_id,
  case when prod_user.user_gender = 'm' then 1 when prod_user.user_gender = 'f' then 2 else null end gender_id,
  case
    when year(enrollment.created) - prod_user.user_year_of_birth >= 7 and year(enrollment.created) - prod_user.user_year_of_birth <= 99
      then floor((year(enrollment.created) - prod_user.user_year_of_birth)/10)
    else null
  end age_decade,
  case when prod_user.user_level_of_education = 'jhs' then 1 when prod_user.user_level_of_education = 'hs' then 2 when prod_user.user_level_of_education = 'a' then 3 when prod_user.user_level_of_education = 'b' then 4 when prod_user.user_level_of_education = 'm' then 5 when prod_user.user_level_of_education = 'p' then 6 else null end education_level_id,
  case when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ru' then 1 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'da' then 2 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'fa' then 3 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ca' then 4 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'tr' then 5 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'nn' then 6 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'mk' then 7 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'yi' then 8 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'mt' then 9 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ta' then 10 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'be' then 11 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'is' then 12 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sv' then 13 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'nb' then 14 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ms' then 15 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'uz' then 16 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sl' then 17 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'pt' then 18 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'tt' then 19 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'hr' then 20 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sr' then 21 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'tn' then 22 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'id' then 23 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sq' then 24 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'bg' then 25 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'hy' then 26 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'fr' then 27 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'he' then 28 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'gd' then 29 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'rm' then 30 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'fo' then 31 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'es' then 32 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sk' then 33 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'de' then 34 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'hu' then 35 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ts' then 36 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sb' then 37 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'lt' then 38 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sa' then 39 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'st' then 40 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ro' then 41 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'hi' then 42 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ja' then 43 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'vi' then 44 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'lv' then 45 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ar' then 46 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'zh' then 47 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'mr' then 48 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'en' then 49 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'nl' then 50 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'az' then 51 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'et' then 52 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'th' then 53 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ko' then 54 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'fi' then 55 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'ur' then 56 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'cs' then 57 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'gd' then 58 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'af' then 59 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'uk' then 60 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'eu' then 61 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'pl' then 62 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'el' then 63 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'xh' then 64 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'it' then 65 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'sw' then 66 when
  coalesce(
    case
      when instr(prod_course.content_language, '-', 1) > 0
        then substring(prod_course.content_language, 1, instr(prod_course.content_language, '-', 1) - 1)
      else prod_course.content_language
    end,
    'en') = 'zu' then 67 else null end content_language_id,
  case when cm.course_subject = 'Computer Science' then 1 when cm.course_subject = 'Business & Management' then 2 when cm.course_subject = 'Engineering' then 3 when cm.course_subject = 'Data Analysis & Statistics' then 4 when cm.course_subject = 'Social Sciences' then 5 when cm.course_subject = 'Humanities' then 6 when cm.course_subject = 'Economics & Finance' then 7 when cm.course_subject = 'Science' then 8 when cm.course_subject = 'Biology & Life Sciences' then 9 when cm.course_subject = 'History' then 10 when cm.course_subject = 'Physics' then 11 when cm.course_subject = 'Math' then 12 when cm.course_subject = 'Communication' then 13 when cm.course_subject = 'Art & Culture' then 14 when cm.course_subject = 'Education & Teacher Training' then 15 when cm.course_subject = 'Medicine' then 16 when cm.course_subject = 'Health & Safety' then 17 when cm.course_subject = 'Environmental Studies' then 18 when cm.course_subject = 'Philosophy & Ethics' then 19 when cm.course_subject = 'Electronics' then 20 when cm.course_subject = 'Literature' then 21 when cm.course_subject = 'Law' then 22 when cm.course_subject = 'Chemistry' then 23 when cm.course_subject = 'Language' then 24 when cm.course_subject = 'Design' then 25 when cm.course_subject = 'Energy & Earth Sciences' then 26 when cm.course_subject = 'Music' then 27 when cm.course_subject = 'Food & Nutrition' then 28 when cm.course_subject = 'Architecture' then 29 when cm.course_subject = 'Ethics' then 30 else null end subject_id,
  case when cm.pacing_type = 'instructor_paced' then 1 when cm.pacing_type = 'self_paced' then 2 else null end pacing_type_id,
  case when cm.program_type = 'Non-Program' then 1 when cm.program_type = 'MicroMasters' then 2 when cm.program_type = 'Professional Certificate' then 3 when cm.program_type = 'XSeries' then 4 else null end program_type_id,
  count(1) enrollment_count,
  sum(case when enrollment.mode in ('verified', 'credit') then 1 else 0 end) verified_enrollment_count,
  sum(case when aeud.count_engaged_days > 0 then 1 else 0 end) engaged_count,
  sum(case when aeud.count_engaged_days > 0 and enrollment.mode in ('verified', 'credit') then 1 else 0 end) verified_engaged_count,
  sum(case when ccu.passed_timestamp is not null then 1 else 0 end) passed_count,
  sum(case when ccu.passed_timestamp is not null and enrollment.mode in ('verified', 'credit') then 1 else 0 end) verified_passed_count from
            lms_read_replica.student_courseenrollment enrollment join
            production.d_user prod_user
  on prod_user.user_id = enrollment.user_id left outer join
 production.d_course prod_course
  on prod_course.course_id = enrollment.course_id left outer join
 business_intelligence.course_master cm
  on cm.course_id = enrollment.course_id left outer join
 business_intelligence.course_completion_user ccu
  on ccu.user_id = enrollment.user_id
  and ccu.course_id = enrollment.course_id left outer join (
    select user_id, course_id, sum(1) count_engaged_days
    from business_intelligence.activity_engagement_user_daily
    where is_engaged = 1
    group by 1, 2
  ) aeud
  on aeud.user_id = enrollment.user_id
  and aeud.course_id = enrollment.course_id where lower(
  regexp_replace(
    regexp_substr(
      regexp_replace(enrollment.course_id, '^.+?:', ''),
      '^.+?[\+\/]', 1),
    '[\+\/]', '')
  ) in ('cornellx_uqx','tokyotechx','savealifex','harvardxplus','trinityx','imfx','tsinghuax','anux','imf','uthealthsphx','osakaux','utpermianbasinx','louvainx','dartmouthx','wellesleyx','utpermianbasin','etsx','davidsonx','gemsx','kix','hmsglobalacademy','mitprofx','utaustinx','misisx','w3cx','utaustinxprofed','michiganx','mexicox','mitx_pro','afghanx','smithx','rwthx','wasedax','mandarinx','upmcx','whartononline','uchicagox','curtinx','babsonx','mongodbx','edinburghx','uicelandx','usmx','urosariox','usm','tennessee','delftwageningenx','purduex','pekingx','nyif','peking','snux','federicax','mdandersonx','tumx','iitbombayx','wageningenx','kyotox','iux','uqx','juilliardx','princetonx','victoriax','utsanantoniox','utennesseex','ritx','epflx','osakax','kthx','bux','doanex','logycax','idbx','ucsdx','hkustx','oecx','efplx','javerianax','upvalenciax','westonhs','galileox','newcastlex','notredamex','witsx','davidsonnext','delftx','hamiltonx','vjx','um6px','hkpolyux','brownx','acca','ibm','utmbx','irtix','harveymuddx','harvardx','ieeex','perkinsx','mcgillx','hkux','smes','redhat','wbgx','schoolyourself','georgetownx','tbrx','israelx','uc3mx','cooperunion','itmox','iimbx','seakademiex','asux','adelaidex','dartmouth_imtx','rwthaachenx','imperialx','teachforamericax','ethx','microsoft','amnestyinternationalx','sdgacademyx','pennx','utax','rwthtumx','imtx','ubcx','berkleex','uwashingtonx','urfux','mitx','berkeleyx','utarlingtonx','brainlabx','hbkux','linuxfoundationx','fullbridgex','caltechdelftx','uncordobax','ethicon','bitsx','oxfordx','chalmersx','uamx','kyotoux','ricexx','bax','gtx','wharton','aws','mephix','cornellx','tenarisuniversityx','tecdemonterreyx','tenarisuniversity','smithsonianx','ricex','university_of_torontox','kironx','upv','catalystx','imperialbusinessx','utokyox','teacherscollegex','caltechx','wgux','sorbonnex','columbiax','colgatex','kuleuvenx','ucsandiegox') group by 1, 2, 3, 4, 5, 6, 7, 8, 9

