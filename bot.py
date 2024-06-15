from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
from db import create_data
import json

API_TOKEN = 'YOU TOKEN'

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start_command(message: types.Message):
    await message.answer("Напиши данные в формате ... и я тебе отвечу")


@dp.message(lambda message: message.content_type ==  types.ContentType.TEXT)
async def data_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        response = create_data(dt_from, dt_upto, group_type)
        response = json.dumps(response)
        await message.answer(response)
    except:
        await message.answer('Невалидный запос. Пример запроса:{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')


async def main():
    dp.message.register(start_command)
    dp.message.register(data_message)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
