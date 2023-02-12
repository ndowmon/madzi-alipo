import fetch, { Request, Headers } from 'node-fetch';
import PQueue from 'p-queue';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';

const LOCATION_PROPERTY_NAMES = new Set([
  'answerId', // number
  
  'agencyRecordId', // null

  'locationId', // string
  'newSourceName', // string
  'waterSourceType', // string
  'visitDate', // string
  'visitType', // string
  'endResult', // string
  'visitorName', // string
]);

const DETAIL_PROPERTY_NAMES = new Set([
  'answerId', // number
  'formTypeId', // number
  
  'agencyRecordId', // null | string
  
  'answerCode', // string
  'newSourceCode', // string
  'newSourceName', // string
  'waterSourceName', // string
  'waterSourceStatus', // string
  'user', // string
  'insertDate', // string
  'informationSectionBgColor', // string
  
  'canDelete', // boolean
  'canEdit', // boolean
  'isTelephoneSurvey', // boolean

  /**
   * {
   *   id: 1564895,
   *   questionId: 472,
   *   questionText: 'How many strokes to fill a 20L bucket (post repair)?',
   *   showInSection: 2,
   *   controlTypeId: 7,
   *   answerText: '48', // if blank or '', do not add.
   *   groupId: 95,
   *   orderNum: 7
   * }
   * 
   * column: `${questionId} : ${questionText}` // 
   * row: `${answerText}
   */
  'informationSection', // Question[]
  'activitySection', // Question[]
  'comment', // Question

  /**
   * {
   *   name: 'Rod Centraliser',
   *   quantity: 4,
   *   costMwk: 695,
   *   uniqueIds: ''
   * }
   * 
   * 'part : Stainless Steel Rods : quantity'
   * 'part : Stainless Steel Rods : costMwk'
   */
  'partUsed', // Part[]

  /**
   * {
   *   url: 'https://s3-eu-west-1.amazonaws.com/madzi-alipo-media/water-source-answer-image/20220901_121946_1662027586000.jpg',
   *   thumbUrl: 'https://s3-eu-west-1.amazonaws.com/madzi-alipo-media/water-source-answer-image/Thumb_20220901_121946_1662027586000.jpg'
   * }
   * 
   * url \n\n url
   */
  'imageAnswers', // ImageAnswer[]

  /**
   * {
   *   firstName: 'Joseph',
   *   lastName: 'Smart',
   *   profilePic: '/App_Media/ProfileIcons/profileImage.jpg',
   *   userId: '1745174e-b395-4e27-a959-60099ee51384'
   * }
   * 
   * `${firstName} ${lastName} \n\n ${firstName} ${lastName}`
   */
  'visitStaff', // VisitStaff[]
]);

const NEW_SOURCE_PROPERTY_NAMES = new Set([
  'newSourceAnswerId', // number
  'code', // string
  'newSourceName', // string
  'agencyRecordId', // string
  'mergewatersource', // null
  'canEditNewSourceName', // boolean
  'canViewCommitteeMembers', // boolean
  'waterSourceName', // string
  'waterSourceTypeName', // string
  'waterSourceStatus', // string
  'statusPin', // string

  /**
   * [
   *   -16.000774517152788,
   *   34.828884507381609
   * ]
   */
  'sourceLatLng', // number[]
  'lastVisitDate', // string
  'isFlagged', // boolean
  'canUnflagWaterSource', // boolean
  'isCommitteeAvailable', // string
  'isFundAvailable', // string
  'installedBy', // string
  'installDate', // string
  'DTAW', // string
  'TTAW', // string
  'zoneMembership', // object[]
  'committeeMembers', // object[]
  'informationSectionBgColor', // string
  'informationSection', // object[]
  'activitySection', // object[]
  'agencies', // object[]
  'formAnswers', // object[]
]);

async function main() {
  let locationTable = [];

  const agencies = await getAgencies();
  const queue = new PQueue({ concurrency: 5 });

  for (let year = 2001; year <= 2022; year ++) {
    const yearDir = join('.', 'data', `${year}`);
    if (!existsSync(yearDir)) mkdirSync(yearDir);
    for (const { Id: agencyId, name: agencyName } of agencies) {
      const path = join(yearDir, `${agencyName}.json`.replace('/', '-'));
      queue.add(async () => {
        if (!existsSync(path)) {
          console.log(`Fetching breakdown data for year ${year}, agency ${agencyName}`);
          const locationTableForYear = await getLocationTableForYearAndAgency({year, agencyId, agencyName});
          writeFileSync(path, JSON.stringify(locationTableForYear.map((row) => ({ ...row, agencyId, agencyName})), null, 2));
        } else {
          console.log(`Reading file ${path} for breakdown data for year ${year}, agency ${agencyName}`);
        }
        locationTable = locationTable.concat(JSON.parse(readFileSync(path, 'utf-8')));
      }).catch((err) => console.error(err));
    }
  }
  await queue.onIdle();

  console.log(locationTable.length);

  writeFileSync(join('.', 'data', 'data.csv'), convertToCSV(locationTable));
}

async function getBearerToken() {
  const token = process.env.BEARER_TOKEN;
  if (!token) throw new Error('The BEARER_TOKEN environment variable must be set in order to run this program');
  return token;
}

function enforcePropertyNames(obj, allowedPropertyNames) {
  Object.keys(obj).forEach((k) => {
    if (!allowedPropertyNames.has(k)) console.log(k);
  });
}

/**
 * Id: number
 * address: string
 * agencyAdditionalPinpoints: { additionalPinpointId: number, name: string }[]
 * agencyCode: string
 * agencyWaterSources: { waterSourceId: number, name: string }[]
 * comment: string
 * name: string
 * phone: string
 */
async function getAgencies() {
  const token = await getBearerToken();
  const headers = new Headers({ authorization: `Bearer ${token}`});
  const response = await fetch(
    new Request('https://www.madzialipo.org/api/Api_Agency/GetAgencies'),
    { headers },
  );

  const responseJson = await response.json();

  console.log(`Length of agencies ${responseJson.length}`)
  return responseJson;
}

async function getNewSourceAnswerDetail(options) {
  const { newSourceCode } = options;
  const token = await getBearerToken();
  const headers = new Headers({ authorization: `Bearer ${token}`});
  const response = await fetch(
    new Request(`https://www.madzialipo.org/api/Api_NewSourceAnswers/GetNewSourceAnswerDetail/?newSourceAnswerCode=${newSourceCode}`),
    { headers },
  );
  const responseJson = await response.json();
  enforcePropertyNames(responseJson, NEW_SOURCE_PROPERTY_NAMES);
  return responseJson;
}

async function getLocationTableForYearAndAgency(options) {
  const { year, agencyId, agencyName } = options;
  const token = await getBearerToken();
  const headers = new Headers({ authorization: `Bearer ${token}`});
  const response = await fetch(new Request(
    'https://www.madzialipo.org/api/Api_Report/GetReportData/?' + Object.entries({
      selectedReportType: '5',
      agencyId: `[${agencyId}]`,
      agencyUser: '[]',
      fromDate: `${year}-01-01T00:00:00.000Z`,
      toDate: `${year}-12-31T23:59:59.999Z`,
      waterSource: '["1_t"]',
      indicatorID: '[]',
      countryId: '',
      districtZoneId: '',
      localZoneId: '',
      regionZoneId: '',
      radius: '',
    }).reduce((queryString, [key, value]) => queryString + '&' + key + '=' + value, ''),
    { headers },
  ));
  const responseJson = await response.json();

  console.log(`Length of table for year ${year}, agency ${agencyName}: ${responseJson.length}`)
  return populateLocationTable(responseJson);
}

/**
 * Map<newSourceAnswerCode, number[]>
 */
const newSourceLatLongMap = new Map();

async function populateLocationTable(locationTable) {
  const token = await getBearerToken();
  const headers = new Headers({ authorization: `Bearer ${token}`});

  const queue = new PQueue({ concurrency: 25 });

  const newLocationTable = [];
  let encounteredErrors = false;
  for (const location of locationTable) {
    enforcePropertyNames(location, LOCATION_PROPERTY_NAMES);

    queue.add(async () => {
      const detailResponse = await fetch(
        `https://www.madzialipo.org/api/Api_NewSourceAnswers/GetAnswerDetail/?answerId=${location.answerId}`,
        { headers },
      );
      console.log(`Response: ${location.answerId}`)
      const detail = await detailResponse.json();

      enforcePropertyNames(detail, DETAIL_PROPERTY_NAMES);

      if (!newSourceLatLongMap.has(detail.newSourceCode)) {
        const newSourceAnswerDetail = await getNewSourceAnswerDetail({ newSourceCode: detail.newSourceCode });
        newSourceLatLongMap.set(detail.newSourceCode, newSourceAnswerDetail.sourceLatLng);
      }

      const newSourceLatLong = newSourceLatLongMap.get(detail.newSourceCode);
      let latitude, longitude;
      try {
        [ latitude, longitude ] = newSourceLatLong;
      } catch (err) {
        console.log(`Could not retrieve lat/long for newSourceCode: ${detail.newSourceCode}`)
      }

      newLocationTable.push({
        ...location,

        answerId: detail.answerId,
        formTypeId: detail.formTypeId,
        agencyRecordId: detail.agencyRecordId,
        answerCode: detail.answerCode,
        newSourceCode: detail.newSourceCode,
        newSourceName: detail.newSourceName,
        waterSourceName: detail.waterSourceName,
        waterSourceStatus: detail.waterSourceStatus,
        user: detail.user,
        insertDate: detail.insertDate,
        informationSectionBgColor: detail.informationSectionBgColor,
        canDelete: detail.canDelete,
        canEdit: detail.canEdit,
        isTelephoneSurvey: detail.isTelephoneSurvey,

        ...toQuestionMap(detail.informationSection),
        ...toQuestionMap(detail.activitySection),
        ...toQuestionMap([detail.comment]),

        
        imageUrls: detail.imageAnswers.map((a) => a.url).join('\n'),
        visitStaff: detail.visitStaff.map((s) => `${s.firstName} ${s.lastName}`).join('\n'),
        ...toPartsUsedMap(detail.partUsed),

        latitude,
        longitude,
      });
      
    }).catch((err) => {
      encounteredErrors = true;
      console.error(err);
    })
  }

  await queue.onIdle();

  return newLocationTable;
}

function toQuestionMap(questions) {
  const questionMap = {};
  for (const question of questions) {
    if (!question) continue;
    questionMap[`${question.questionId} : ${question.questionText}`] = question.answerText;
  }
  return questionMap;
}

function toPartsUsedMap(partsUsed) {
  const partsUsedMap = {};
  for (const partUsed of partsUsed) {
    partsUsedMap[`part : ${partUsed.name} : quantity`] = partUsed.quantity;
    partsUsedMap[`part : ${partUsed.name} : costMwk`] = partUsed.costMwk;
  }
  return partsUsedMap;
}

function convertToCSV(arr) {
  const keysSet = new Set([]);

  for (const obj of arr) {
    for (const [key, value] of Object.entries(obj)) {
      if (value) keysSet.add(key);
    }
  }

  const keysArray = [...keysSet];

  const csvArray = [];
  for (const obj of arr) {
    let row = [];
    for (const key of keysArray) {
      row.push('"' + (obj[key] === undefined ? '' : obj[key]) + '"');
    }
    csvArray.push(row);
    // break;
  }
  csvArray.unshift(keysArray.map((k) => '"' + k + '"'))

  return csvArray.map((obj) => {
    return Object.values(obj).join(',')
  }).join('\n')
}

main().then().catch((err) => console.error(err));

