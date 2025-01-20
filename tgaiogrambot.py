from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
import asyncio
import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import asyncpg
from functools import wraps
from datetime import datetime
import random
import aiocron


# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN or not DATABASE_URL:
    raise ValueError("BOT_TOKEN and DATABASE_URL must be set in environment variables")

ALLOWED_USERS = [2041928302, 6635421234, 6137303580]
glava = [2041928302]
PUBLIC_CHANNELS = ["@MeminoMem"]
user_luck = {}
otp_video = {}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
db_pool = None


# Utility Functions
async def send_message(chat_id, text):
    await bot.send_message(chat_id=chat_id, text=text)


# Database Initialization
async def init_db_pool():
    global db_pool
    retries = 3
    for attempt in range(retries):
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Database connection pool created successfully.")
            break
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}: Failed to connect to the database: {e}")
            if attempt == retries - 1:
                raise e


async def close_db_pool():
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None
        logger.info("Database connection pool closed.")
    else:
        logger.warning("Database pool was not initialized, nothing to close.")


async def create_tables():
    if not db_pool:
        logger.error("Database pool is not initialized.")
        return

    async with db_pool.acquire() as conn:
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS memes (
                    id SERIAL PRIMARY KEY,
                    meme_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stickers (
                    id SERIAL PRIMARY KEY,
                    sticker_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS voice_messages (
                    id SERIAL PRIMARY KEY,
                    voice_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS user_content (
                    user_id BIGINT NOT NULL,
                    content_id TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    UNIQUE (user_id, content_id, content_type, source)
                );
                CREATE TABLE IF NOT EXISTS content_feedback (
                    id SERIAL PRIMARY KEY,
                    content_id TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    likes INTEGER DEFAULT 0,
                    dislikes INTEGER DEFAULT 0,
                    UNIQUE (content_id, content_type)
                );
                CREATE TABLE IF NOT EXISTS user_feedback (
                    user_id BIGINT NOT NULL,
                    content_id TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    feedback_type TEXT NOT NULL, -- 'like' или 'dislike'
                    PRIMARY KEY (user_id, content_id, content_type)
                );
                CREATE TABLE IF NOT EXISTS bot_users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    joined_at TIMESTAMP DEFAULT NOW()
                );
            """)
            logger.info("Tables created successfully.")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")


async def update_tables():
    if not db_pool:
        logger.error("Database pool is not initialized.")
        return

    async with db_pool.acquire() as conn:
        try:
            # Проверяем и добавляем поле created_at, если его нет
            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'user_content' AND column_name = 'created_at'
                    ) THEN
                        ALTER TABLE user_content ADD COLUMN created_at TIMESTAMP DEFAULT NOW();
                    END IF;
                END $$;
            """)
            logger.info("Table user_content updated successfully.")
        except Exception as e:
            logger.error(f"Error updating tables: {e}")


# Subscription Check
async def is_subscribed(user_id: int) -> bool:
    for channel in PUBLIC_CHANNELS:
        try:
            status = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if status.status not in ["member", "administrator", "creator"]:
                return False
        except Exception as e:
            print(f"Error checking channel {channel}: {e}")
            return False
    return True

# Декоратор для проверки подписки
def subscription_required(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        user_id = message.from_user.id
        if await is_subscribed(user_id):
            return await handler(message, *args, **kwargs)
        else:
            # Динамическое создание кнопок для каждого канала
            markup = InlineKeyboardMarkup()
            for channel in PUBLIC_CHANNELS:
                channel_link = f"https://t.me/{channel.lstrip('@')}"
                markup.row(InlineKeyboardButton(channel, url=channel_link))
            markup.row(
                InlineKeyboardButton('\u2705 Проверить подписку', callback_data='check_subscription')
            )
            await message.reply("Для работы бота требуется подписка на эти каналы:", reply_markup=markup)
    return wrapper


async def send_content(message: types.Message, content_type: str, table_name: str, uid: int = None,
                       source: str = "command"):
    user_id = message.from_user.id
    today = datetime.now().date()  # Текущая дата

    try:
        async with db_pool.acquire() as conn:
            # Пропускаем проверку лимита для ALLOWED_USERS
            if user_id not in ALLOWED_USERS:
                # Проверяем, сколько раз пользователь уже получил данный тип контента сегодня
                daily_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM user_content
                    WHERE user_id = $1 AND content_type = $2 AND source = $3 AND DATE(created_at) = $4
                """, user_id, content_type, source, today)

                if daily_count >= 15:
                    await message.reply(
                        f"Вы достигли дневного лимита в 15 {content_type} за сегодня. Попробуйте завтра.")
                    return

            # Выбор контента
            if uid is not None:
                result = await conn.fetchrow(f"""
                    SELECT {content_type}_id FROM {table_name} WHERE id = $1
                """, uid)
            else:
                result = await conn.fetchrow(f"""
                    SELECT v.id, v.{content_type}_id FROM {table_name} v
                    LEFT JOIN user_content uc 
                    ON v.{content_type}_id = uc.content_id 
                    AND uc.user_id = $1 
                    AND uc.content_type = $2
                    AND uc.source = $3
                    WHERE uc.content_id IS NULL
                    ORDER BY RANDOM() LIMIT 1
                """, user_id, content_type, source)

            if result:
                content_id = result[f"{content_type}_id"]
                uid = uid or result["id"]

                # Получение лайков/дизлайков
                feedback = await conn.fetchrow("""
                    SELECT likes, dislikes FROM content_feedback
                    WHERE content_id = $1 AND content_type = $2
                """, content_id, content_type)

                likes = feedback['likes'] if feedback else 0
                dislikes = feedback['dislikes'] if feedback else 0

                # Создаём клавиатуру
                keyboard = InlineKeyboardMarkup()
                keyboard.row(
                    InlineKeyboardButton(f"👍 {likes}", callback_data=f"like_{content_type}_{uid}"),
                    InlineKeyboardButton(f"👎 {dislikes}", callback_data=f"dislike_{content_type}_{uid}")
                )

                keyboard.add(InlineKeyboardButton("➡️ Следующее", callback_data=f"next_{content_type}"))

                # Отправляем контент
                if content_type == "video":
                    await bot.send_video(message.chat.id, content_id, reply_markup=keyboard)
                elif content_type == "meme":
                    await bot.send_photo(message.chat.id, content_id, reply_markup=keyboard)
                elif content_type == "sticker":
                    await bot.send_sticker(message.chat.id, content_id, reply_markup=keyboard)
                elif content_type == "voice":
                    await bot.send_voice(message.chat.id, content_id, reply_markup=keyboard)

                # Сохраняем просмотр контента
                await conn.execute("""
                    INSERT INTO user_content (user_id, content_id, content_type, source, created_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT DO NOTHING
                """, user_id, content_id, content_type, source)
            else:
                await message.reply(f"No available {content_type} to send.")

    except Exception as e:
        logger.error(f"Error getting {content_type}: {e}")
        await message.reply(f"Error retrieving {content_type}: {e}")


# Инициализация MemoryStorage
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# Определение состояний
class AddContentState(StatesGroup):
    waiting_for_content = State()


# Команда для начала добавления контента
@dp.message_handler(commands=['addvideo', 'addmeme', 'addsticker', 'addvoice'])
async def start_adding_content(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав для добавления контента.")
        return

    # Определяем тип контента из команды
    command = message.text.split()[0][1:]  # Убираем "/"
    content_type = command[3:]  # Убираем "add" из команды

    # Сохраняем тип контента в состоянии
    await state.update_data(content_type=content_type)
    await AddContentState.waiting_for_content.set()

    await message.reply(f"Отправьте {content_type}, чтобы я его сохранил.")


# Обработчик для получения контента
@dp.message_handler(state=AddContentState.waiting_for_content, content_types=types.ContentTypes.ANY)
async def add_content(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    content_type = user_data.get('content_type')
    table_name = None
    content_id = None

    # Определяем таблицу и получаем ID контента
    if content_type == "video" and message.video:
        table_name = "videos"
        content_id = message.video.file_id
    elif content_type == "meme" and message.photo:
        table_name = "memes"
        content_id = message.photo[-1].file_id
    elif content_type == "sticker" and message.sticker:
        table_name = "stickers"
        content_id = message.sticker.file_id
    elif content_type == "voice" and message.voice:
        table_name = "voice_messages"
        content_id = message.voice.file_id

    if not table_name or not content_id:
        await message.reply("Отправленный контент не подходит. Попробуйте снова.")
        return

    # Сохранение контента в базу данных
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {table_name} ({content_type}_id) VALUES ($1)
                ON CONFLICT DO NOTHING
            """, content_id)
        await message.reply(f"{content_type.capitalize()} успешно добавлено.")
    except Exception as e:
        logger.error(f"Ошибка при добавлении {content_type}: {e}")
        await message.reply(f"Не удалось добавить {content_type}: {e}")

    # Завершаем состояние
    await state.finish()

@dp.message_handler(commands=['dobro'])
async def dobavit_admina(message: types.Message):
    user_id = message.from_user.id
    global ALLOWED_USERS
    if user_id not in glava:
        await send_message(message.chat.id, 'У вас нет прав для этой команды')
        return
    try:
        user_input = message.text.split(maxsplit=1)[1]
        ID_POLZOVATELYA = int(user_input)
        if ID_POLZOVATELYA not in ALLOWED_USERS:
            ALLOWED_USERS.append(ID_POLZOVATELYA)
            await send_message(message.chat.id, 'ID добавлен в бота как админ')
        else:
            await send_message(message.chat.id, 'Так он ж и так админ че хочешь')
    except(IndexError, ValueError):
        # Ошибка, если пользователь не ввёл число или текст некорректен
        await message.reply("Пожалуйста, введите число после команды. Пример: /dobro 123")

@dp.message_handler(commands=['pshlnx'])
async def dinax(message: types.Message):
    user_id = message.from_user.id
    if user_id not in glava:
        await send_message(message.chat.id, 'Пошел нах отсюда')
        return

    try:
        global ALLOWED_USERS
        user_input = message.text.split(maxsplit=1)[1]
        ID_POLZOVATELYA = int(user_input)
        ALLOWED_USERS.remove(ID_POLZOVATELYA)
        await send_message(message.chat.id, 'Ура! теперь стало меньше на одного чупиздрика в админах')
    except(IndexError, ValueError):
        # Ошибка, если пользователь не ввёл число или текст некорректен
        await message.reply("Пожалуйста, введите число после команды. Пример: /pshlnx 123")

@dp.message_handler(commands=['spisok_ebanko'])
async def spisok_ebanko(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await send_message(message.chat.id, 'Ты слишком слаю для этой команды')
        return

    await send_message(message.chat.id, f'вот список который ты так хочешь {ALLOWED_USERS}')

@dp.message_handler(commands=['add_channel'])
async def add_channel_command(message: types.Message):
    user_id = message.from_user.id
    global PUBLIC_CHANNELS
    if user_id not in ALLOWED_USERS:
        await message.reply("Че ты хочешь добавить канал да?\nА фиг тобе,\nТЫ НЕВЛАСТНЫЙ ТУТ!")
        return
    try:
        # Получение имени канала от пользователя
        channel = message.text.split(maxsplit=1)[1].strip()
        if not channel.startswith('@'):
            await message.reply("Название канала должно начинаться с '@'. Пример: /add_channel @example_channel")
            return
        if channel in PUBLIC_CHANNELS:
            await message.reply(f"Канал {channel} уже есть в списке.")
        else:
            PUBLIC_CHANNELS.append(channel)
            await message.reply(f"Канал {channel} добавлен в список проверки.")
    except IndexError:
        await message.reply("Пожалуйста, укажите название канала. Пример: /add_channel @example_channel")


@dp.message_handler(commands=['minus_channel'])
async def minus_channel_command(message: types.Message):
    user_id = message.from_user.id
    global PUBLIC_CHANNELS
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав, да кто ты такой вообще?")
        return
    try:
        # Получение имени канала от пользователя
        channel = message.text.split(maxsplit=1)[1].strip()
        if not channel.startswith('@'):
            await message.reply("Название канала должно начинаться с '@'. Пример: /minus_channel @example_channel")
            return
        if channel in PUBLIC_CHANNELS:
            PUBLIC_CHANNELS.remove(channel)
            await message.reply(f"Канал {channel} теперь нет в списке.")
        else:
            await message.reply(f"Канал {channel} не было в списке")
    except IndexError:
        await message.reply("Пожалуйста, укажите название канала. Пример: /add_channel @example_channel")

# Команда для просмотра списка каналов
@dp.message_handler(commands=['list_channels'])
async def list_channels_command(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("че за хуйня")
        return
    if PUBLIC_CHANNELS:
        channels = '\n'.join(PUBLIC_CHANNELS)
        await message.reply(f"Список каналов для проверки:\n{channels}")
    else:
        await message.reply("Список каналов пуст.")



@dp.message_handler(commands=['menu'])
@subscription_required
async def show_menu(message: types.Message):
    # Создаем клавиатуру
    menu_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    menu_keyboard.add(
        KeyboardButton('🎥 Видео'),
        KeyboardButton('🖼️ Мемы')
    )
    menu_keyboard.add(
        KeyboardButton('📦 Стикеры'),
        KeyboardButton('🎙️ Голосовухи')
    )
    menu_keyboard.add(
        KeyboardButton('🍀 Узнать уровень удачи'),
    )
    await message.reply("Выберите категорию:", reply_markup=menu_keyboard)


@dp.message_handler(commands=['start'])
@subscription_required
async def privetsvie(message: types.Message):
    await bot.send_message(message.chat.id,
                           'Приветствую вас в нашем боте!\nБот умеет присылать вам прикольные видео, мемы, стикеры, смешные голосовые сообщение)\nПриятного пользования нашим ботом!\nУдачи!!!')


@dp.message_handler(
    lambda message: message.text in ['🎥 Видео', '🖼️ Мемы', '📦 Стикеры', '🎙️ Голосовухи', '🍀 Узнать уровень удачи'])
async def handle_menu_selection(message: types.Message):
    if message.text == '🎥 Видео':
        await handle_video_command(message)
    elif message.text == '🖼️ Мемы':
        await handle_memes_command(message)
    elif message.text == '📦 Стикеры':
        await handle_sticker(message)
    elif message.text == '🎙️ Голосовухи':
        await handle_voice(message)
    elif message.text == '🍀 Узнать уровень удачи':
        await luck(message)


@dp.callback_query_handler(lambda c: c.data == 'check_subscription')
async def check_subscription_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    msg = callback_query.message  # Объект сообщения
    if await is_subscribed(user_id):
        await callback_query.answer("Вы подписаны!", show_alert=True)

        # Удаление сообщения
        await msg.delete()

        await send_message(
            msg,
            'Теперь вы можете использовать бота\nЕсть два способа использования бота\nПервый способ через "меню" которое находится рядом с кнопками отправки сообщения\nВторой способ через такие команды как /menu, /video, /memes и т.д.'
        )
        await show_menu(msg)  # Передаём объект сообщения
    else:
        await callback_query.answer(
            "Пожалуйста, подпишитесь на каналы, чтобы бот работал.", show_alert=True
        )


@dp.message_handler(commands=["video"])
@subscription_required
async def handle_video_command(message: types.Message):
    args = message.get_args()
    uid = int(args) if args and args.isdigit() else None
    await send_content(message, "video", "videos", uid, "command")


@dp.message_handler(commands=['memes'])
@subscription_required
async def handle_memes_command(message: types.Message):
    args = message.get_args()
    uid = int(args) if args and args.isdigit() else None
    await send_content(message, "meme", "memes", uid, "command")


@dp.message_handler(commands=['stickers', 's'])
@subscription_required
async def handle_sticker(message: types.Message):
    args = message.get_args()
    uid = int(args) if args and args.isdigit() else None
    await send_content(message, "sticker", "stickers", uid, "command")


@dp.message_handler(commands=['voice', 'vo'])
@subscription_required
async def handle_voice(message: types.Message):
    args = message.get_args()
    uid = int(args) if args and args.isdigit() else None
    await send_content(message, "voice", "voice_messages", uid, "command")


@dp.message_handler(commands=['luck'])
@subscription_required
async def luck(message: types.Message):
    user_id = message.from_user.id
    today = datetime.now().date()

    # Если пользователь уже выполнял команду сегодня
    if user_id in user_luck and user_luck[user_id]['date'] == today:
        luck_score = user_luck[user_id]['luck']
        response = f"Твой действительный уровень удачи на сегодня уже определён: {luck_score / 2}% \U0001F340"
    else:
        # Выполняем 10 раз для среднего результата
        total_luck = 0
        for _ in range(10):
            luck_score = random.randint(1, 200)
            total_luck += luck_score

        # Считаем средний результат
        average_luck = total_luck // 10
        user_luck[user_id] = {'luck': average_luck, 'date': today}

        # Определяем текст и эмодзи на основе среднего уровня удачи
        # Определяем текст и эмодзи на основе среднего уровня удачи
        if average_luck <= 22:
            emoji = "\U0001F622"
            comments = [
                "Сегодня совсем не повезло. Отдохни и попробуй завтра.",
                "Не бери всё близко к сердцу, завтра будет лучше.",
                "Кажется, удача сегодня решила взять выходной.",
                "Лучше не принимать важных решений. Просто отдохни.",
                "Тяжёлый день, но это временно. Не сдавайся!"
            ]
        elif average_luck <= 60:
            emoji = "\U0001F641"
            comments = [
                "Не самый удачный день, но всё можно исправить.",
                "День не слишком удачный, но он всё же твой.",
                "Проблемы приходят и уходят. Завтра будет лучше.",
                "Будь осторожен, но не теряй надежды.",
                "Сегодняшний день учит терпению. Это тоже важно!"
            ]
        elif average_luck <= 100:
            emoji = "\U0001F610"
            comments = [
                "День ниже среднего, но в твоих силах сделать его лучше.",
                "Не лучший день, но он всё же движется вперёд.",
                "Иногда просто плыть по течению — лучший выбор.",
                "Пусть это будет день отдыха и размышлений.",
                "Не ожидай слишком многого, и ты избежишь разочарований."
            ]
        elif average_luck <= 140:
            emoji = "\U0001F642"
            comments = [
                "Средний уровень удачи. Всё идёт своим чередом.",
                "Хороший день для небольших достижений.",
                "Не торопись, и всё получится.",
                "День пройдёт ровно, наслаждайся этим моментом.",
                "Идеальное время для планирования и подготовки."
            ]
        elif average_luck <= 160:
            emoji = "\U0001F603"
            comments = [
                "День с хорошим потенциалом. Используй его!",
                "Удача с тобой, лови момент.",
                "Прекрасный день для новых идей и проектов.",
                "Всё получится, главное — верить в себя.",
                "Ты на правильном пути. Двигайся вперёд!"
            ]
        elif average_luck <= 190:
            emoji = "\U0001F604"
            comments = [
                "Отличный день для свершений. Всё в твоих руках!",
                "Ты словно магнит для удачи сегодня!",
                "Всё, за что ты берёшься, приносит успех.",
                "Смело берись за сложные задачи — они тебе по плечу.",
                "Этот день обещает быть незабываемым. Наслаждайся!"
            ]
        else:
            emoji = "\U0001F60D"
            comments = [
                "Ты просто невероятно удачлив! Воспользуйся этим шансом.",
                "Сегодня твой день! Всё складывается идеально.",
                "Кажется, сама Вселенная работает на тебя.",
                "Удача улыбается тебе во всём. Не упусти этот момент!",
                "Ты на вершине мира! Всё получается легко и просто."
            ]
        comment = random.choice(comments)
        response = f"Сегодня твой средний уровень удачи: {average_luck / 2}% {emoji}\n{comment}"

    await message.reply(response)


@dp.callback_query_handler(lambda c: c.data.startswith(('like_', 'dislike_')))
async def handle_like_dislike(callback_query: types.CallbackQuery):
    data = callback_query.data.split('_')
    action = data[0]  # 'like' или 'dislike'
    content_type = data[1]
    uid = int(data[2])
    user_id = callback_query.from_user.id

    try:
        async with db_pool.acquire() as conn:
            # Получаем content_id по uid
            result = await conn.fetchrow(f"""
                SELECT {content_type}_id FROM {content_type}s WHERE id = $1
            """, uid)
            if not result:
                await callback_query.answer("Контент не найден.", show_alert=True)
                return

            content_id = result[f"{content_type}_id"]

            # Проверяем, голосовал ли пользователь за этот контент
            feedback_check = await conn.fetchrow("""
                SELECT feedback_type FROM user_feedback
                WHERE user_id = $1 AND content_id = $2 AND content_type = $3
            """, user_id, content_id, content_type)

            if feedback_check:
                await callback_query.answer("Вы уже голосовали за этот контент!", show_alert=True)
                return

            # Добавляем голос
            if action == 'like':
                await conn.execute("""
                    INSERT INTO content_feedback (content_id, content_type, likes)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (content_id, content_type)
                    DO UPDATE SET likes = content_feedback.likes + 1
                """, content_id, content_type)
                feedback_type = 'like'
            elif action == 'dislike':
                await conn.execute("""
                    INSERT INTO content_feedback (content_id, content_type, dislikes)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (content_id, content_type)
                    DO UPDATE SET dislikes = content_feedback.dislikes + 1
                """, content_id, content_type)
                feedback_type = 'dislike'

            # Сохраняем информацию о голосовании пользователя
            await conn.execute("""
                INSERT INTO user_feedback (user_id, content_id, content_type, feedback_type)
                VALUES ($1, $2, $3, $4)
            """, user_id, content_id, content_type, feedback_type)

            # Получаем обновленные данные лайков/дизлайков
            feedback = await conn.fetchrow("""
                SELECT likes, dislikes FROM content_feedback
                WHERE content_id = $1 AND content_type = $2
            """, content_id, content_type)
            likes = feedback['likes']
            dislikes = feedback['dislikes']

            # Обновляем клавиатуру
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton(f"👍 {likes}", callback_data=f"like_{content_type}_{uid}"),
                InlineKeyboardButton(f"👎 {dislikes}", callback_data=f"dislike_{content_type}_{uid}")
            )
            keyboard.add(InlineKeyboardButton("➡️ Следующее", callback_data=f"next_{content_type}"))

            # Редактируем сообщение
            await bot.edit_message_reply_markup(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                reply_markup=keyboard
            )

            await callback_query.answer("Ваш голос учтён!")
    except Exception as e:
        logger.error(f"Ошибка обработки {action}: {e}")
        await callback_query.answer("Ошибка обработки.", show_alert=True)


# Обработчик колбэков для лайков, дизлайков и следующего
@dp.callback_query_handler(lambda c: c.data.startswith(('like_', 'dislike_', 'next_')))
async def handle_callback_query(callback_query: types.CallbackQuery):
    data = callback_query.data.split('_')
    action = data[0]  # like, dislike, next
    content_type = data[1]

    if action == 'next':
        table_map = {
            "video": "videos",
            "meme": "memes",
            "sticker": "stickers",
            "voice": "voice_messages"
        }
        table_name = table_map.get(content_type)

        if table_name:
            await send_content(callback_query.message, content_type=content_type, table_name=table_name,
                               source="callback")
        else:
            await callback_query.answer("Unknown content type.", show_alert=True)


@dp.message_handler(commands=['delete_all_videos'])
async def delete_all_videos(message: types.Message):
    user_id = message.from_user.id
    if user_id not in glava:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM videos")
        await message.reply("Все видео успешно удалены из базы данных.")
    except Exception as e:
        logger.error(f"Ошибка при удалении видео: {e}")
        await message.reply(f"Не удалось удалить видео: {e}")


@dp.message_handler(commands=['delete_all_memes'])
async def delete_all_memes(message: types.Message):
    user_id = message.from_user.id
    if user_id not in glava:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM memes")
        await message.reply("Все мемы успешно удалены из базы данных.")
    except Exception as e:
        logger.error(f"Ошибка при удалении мемов: {e}")
        await message.reply(f"Не удалось удалить мемы: {e}")


@dp.message_handler(commands=['delete_all_stickers'])
async def delete_all_stickers(message: types.Message):
    user_id = message.from_user.id
    if user_id not in glava:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM stickers")
        await message.reply("Все стикеры успешно удалены из базы данных.")
    except Exception as e:
        logger.error(f"Ошибка при удалении стикеров: {e}")
        await message.reply(f"Не удалось удалить стикеры: {e}")


@dp.message_handler(commands=['delete_all_voice'])
async def delete_all_voice(message: types.Message):
    user_id = message.from_user.id
    if user_id not in glava:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM voice_messages")
        await message.reply("Все голосовые сообщения успешно удалены из базы данных.")
    except Exception as e:
        logger.error(f"Ошибка при удалении голосовых сообщений: {e}")
        await message.reply(f"Не удалось удалить голосовые сообщения: {e}")


@dp.message_handler(commands=['get_all_video_ids'])
async def get_all_video_ids(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT video_id FROM videos")
            if rows:
                video_ids = [row["video_id"] for row in rows]
                await send_in_chunks(message, "Сохраненные видео ID:\n", video_ids)
            else:
                await message.reply("База данных не содержит видео.")
    except Exception as e:
        logger.error(f"Ошибка при получении ID видео: {e}")
        await message.reply(f"Не удалось получить ID видео: {e}")


@dp.message_handler(commands=['get_all_memes_ids'])
async def get_all_memes_ids(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT meme_id FROM memes")
            if rows:
                meme_ids = [row["meme_id"] for row in rows]
                await send_in_chunks(message, "Сохраненные мемы ID:\n", meme_ids)
            else:
                await message.reply("База данных не содержит мемов.")
    except Exception as e:
        logger.error(f"Ошибка при получении ID мема: {e}")
        await message.reply(f"Не удалось получить ID мемов: {e}")


@dp.message_handler(commands=['get_all_stickers_ids'])
async def get_all_stickers_ids(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT sticker_id FROM stickers")
            if rows:
                sticker_ids = [row["sticker_id"] for row in rows]
                await send_in_chunks(message, "Сохраненные стикеры ID:\n", sticker_ids)
            else:
                await message.reply("База данных не содержит стикеров.")
    except Exception as e:
        logger.error(f"Ошибка при получении ID стикера: {e}")
        await message.reply(f"Не удалось получить ID стикеров: {e}")


@dp.message_handler(commands=['get_all_voice_ids'])
async def get_all_voice_ids(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT voice_id FROM voice_messages")
            if rows:
                voice_ids = [row["voice_id"] for row in rows]
                await send_in_chunks(message, "Сохраненные голосовые сообщения ID:\n", voice_ids)
            else:
                await message.reply("База данных не содержит голосовух.")
    except Exception as e:
        logger.error(f"Ошибка при получении ID голоса: {e}")
        await message.reply(f"Не удалось получить ID голосового: {e}")


async def send_in_chunks(message, prefix, data, chunk_size=4096):
    message_chunk = prefix
    for item in data:
        if len(message_chunk) + len(item) + 1 > chunk_size:
            await message.reply(message_chunk)
            message_chunk = ""
        message_chunk += f"{item}\n"
    if message_chunk:
        await message.reply(message_chunk)


class BroadcastState(StatesGroup):
    broadcasting = State()


@dp.message_handler(commands=['start'])
async def register_user(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bot_users (user_id, username)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO NOTHING
        """, user_id, username)
    await message.reply("Вы зарегистрированы!")


@dp.message_handler(commands=['otpravka'])
async def start_broadcast(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await message.reply("У вас нет прав для выполнения этой команды.")
        return

    await message.reply("Вы вошли в режим рассылки. Отправьте сообщение, которое нужно разослать всем пользователям.\n"
                        "Когда захотите завершить рассылку, напишите `/stop`.")
    await BroadcastState.broadcasting.set()


@dp.message_handler(state=BroadcastState.broadcasting, commands=['stop'])
async def stop_broadcasting(message: types.Message, state: FSMContext):
    await message.reply("Режим рассылки завершён.")
    await state.finish()


@dp.message_handler(state=BroadcastState.broadcasting, content_types=types.ContentType.ANY)
async def broadcast_message(message: types.Message, state: FSMContext):
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM bot_users")
        count = 0
        for user in users:
            try:
                # Проверяем тип содержимого сообщения
                if message.content_type == 'text':
                    await bot.send_message(chat_id=user['user_id'], text=message.text)

                elif message.content_type == 'photo':
                    await bot.send_photo(chat_id=user['user_id'], photo=message.photo[-1].file_id,
                                         caption=message.caption)

                elif message.content_type == 'video':
                    await bot.send_video(chat_id=user['user_id'], video=message.video.file_id, caption=message.caption)

                elif message.content_type == 'animation':
                    await bot.send_animation(chat_id=user['user_id'], animation=message.animation.file_id,
                                             caption=message.caption)

                elif message.content_type == 'document':
                    await bot.send_document(chat_id=user['user_id'], document=message.document.file_id,
                                            caption=message.caption)

                elif message.content_type == 'audio':
                    await bot.send_audio(chat_id=user['user_id'], audio=message.audio.file_id, caption=message.caption)

                elif message.content_type == 'voice':
                    await bot.send_voice(chat_id=user['user_id'], voice=message.voice.file_id, caption=message.caption)

                elif message.content_type == 'sticker':
                    await bot.send_sticker(chat_id=user['user_id'], sticker=message.sticker.file_id)

                else:
                    await bot.send_message(chat_id=user['user_id'], text="Этот тип сообщения не поддерживается.")

                count += 1

            except Exception as e:
                logging.error(f"Failed to send message to {user['user_id']}: {e}")

    await message.reply(f"Сообщение успешно отправлено {count} пользователям.")


@aiocron.crontab('0 12 * * *')  # Каждый день в 12:00
async def scheduled_daily_video():
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM bot_users")
        for user in users:
            try:
                # Отправляем команду /video от имени бота
                await dp.bot.send_message(chat_id=user['user_id'], text="/video")
            except Exception as e:
                logger.error(f"Ошибка при отправке ежедневного видео пользователю {user['user_id']}: {e}")


@dp.message_handler(commands=['content_count'])
async def content_count(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        await message.reply("У вас нет прав на выполнение этой команды.")
        return

    try:
        async with db_pool.acquire() as conn:
            # Подсчёт видео
            video_count = await conn.fetchval("SELECT COUNT(*) FROM videos")
            # Подсчёт мемов
            meme_count = await conn.fetchval("SELECT COUNT(*) FROM memes")
            # Подсчёт стикеров
            sticker_count = await conn.fetchval("SELECT COUNT(*) FROM stickers")
            # Подсчёт голосовых
            voice_count = await conn.fetchval("SELECT COUNT(*) FROM voice_messages")

        # Формируем ответ
        response = (
            f"📊 **Статистика контента:**\n"
            f"🎥 Видео: {video_count or 0}\n"
            f"🖼️ Мемы: {meme_count or 0}\n"
            f"🖼️ Стикеры: {sticker_count or 0}\n"
            f"🎙️ Голосовые: {voice_count or 0}"
        )
        await message.reply(response, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка при подсчёте контента: {e}")
        await message.reply(f"Не удалось получить статистику: {e}")


async def main():
    # Инициализация базы данных
    await init_db_pool()
    await create_tables()
    await update_tables()
    aiocron.crontab('0 12 * * *')(scheduled_daily_video)
    # Запуск бота
    try:
        await dp.start_polling()
    finally:
        await close_db_pool()


if __name__ == '__main__':
    # Запуск основного цикла событий
    asyncio.run(main())
    executor.start_polling(dp, skip_updates=True)  # Рекомендуется для большинства случаев
