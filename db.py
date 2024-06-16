from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("localhost", 27017)
db = client.testovoe
collection = db.sample_collection

async def create_data(dt_from, dt_upto, group_by):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    # Проверяем какая группировка выбрана
    if group_by == 'month':
        group_stage_fields = {
            'year': {'$year': '$dt'},
            'month': {'$month': '$dt'}
        }
        format_label = lambda x: f"{x['year']:04d}-{x['month']:02d}-01T00:00:00"

    elif group_by == 'day':
        group_stage_fields = {
            'year': {'$year': '$dt'},
            'month': {'$month': '$dt'},
            'day': {'$dayOfMonth': '$dt'}
        }
        format_label = lambda x: f"{x['year']:04d}-{x['month']:02d}-{x['day']:02d}T00:00:00"

    elif group_by == 'hour':
        group_stage_fields = {
            'year': {'$year': '$dt'},
            'month': {'$month': '$dt'},
            'day': {'$dayOfMonth': '$dt'},
            'hour': {'$hour': '$dt'}
        }
        format_label = lambda x: f"{x['year']:04d}-{x['month']:02d}-{x['day']:02d}T{x['hour']:02d}:00:00"
    else:
        raise ValueError("no sort type matches")

    # Фильтруем по дате
    match_stage = {
        '$match': {
            'dt': {
                '$gte': dt_from,
                '$lte': dt_upto
            }
        }
    }

    # Группируем отфильтрованные значения
    group_stage = {
        '$group': {
            '_id': group_stage_fields,
            'total_value': {'$sum': '$value'}
        }
    }

    # Сортируем сгруппированные значения
    sort_stage = {
        '$sort': {
            '_id.year': 1,
            '_id.month': 1,
            '_id.day': 1,
            '_id.hour': 1
        }
    }

    pipeline = [match_stage, group_stage, sort_stage]
    result = collection.aggregate(pipeline)

    dataset = []
    labels = []
    result_dict = {}

    # Форматируем результат агрегации в словарь для быстрого доступа
    for doc in result:
        label = format_label(doc['_id'])
        result_dict[label] = doc['total_value']

    current_date = dt_from
    if group_by == 'month':
        # Проходим циклом по всем датам от и до
        while current_date <= dt_upto:
            label = format_label({'year': current_date.year, 'month': current_date.month})
            # Проверяем есть ли такая дата в результирующем массиве
            if label not in result_dict:
                # Если даты нет, то устанавливаем её значение в 0
                result_dict[label] = 0
            # Гарантируем переход в следующий месяц и устанавливаем начало месяца
            next_month = current_date.replace(day=28) + timedelta(days=4)
            current_date = next_month.replace(day=1)
    elif group_by == 'day':
        while current_date <= dt_upto:
            label = format_label({'year': current_date.year, 'month': current_date.month, 'day': current_date.day})
            if label not in result_dict:
                result_dict[label] = 0
            current_date += timedelta(days=1)
    elif group_by == 'hour':
        while current_date <= dt_upto:
            label = format_label({'year': current_date.year, 'month': current_date.month, 'day': current_date.day, 'hour': current_date.hour})
            if label not in result_dict:
                result_dict[label] = 0
            current_date += timedelta(hours=1)

    for label in sorted(result_dict):
        labels.append(label)
        dataset.append(result_dict[label])

    return {"dataset": dataset, "labels": labels}
