class Functions:
    def __init__(self, db_client):
        """
        Инициализирует объект функций и регистрирует действия.
        """
        self.db_client = db_client
        self.action_handlers = {}

        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if callable(attr) and hasattr(attr, "_action_name"):
                action_name = getattr(attr, "_action_name")
                self.action_handlers[action_name] = attr

    def handle_request(self, request):
        """
        Обрабатывает запрос в зависимости от переданной команды.
        """
        action = request.get('action')
        if not action:
            raise ValueError("Поле 'action' обязательно.")

        handler = self.action_handlers.get(action)
        if not handler:
            raise ValueError(f"Неизвестное действие: {action}")

        return handler(request)

    @staticmethod
    def action(name):
        """
        Декоратор для регистрации метода как обработчика действия.
        """
        def wrapper(func):
            func._action_name = name
            return func
        return wrapper

    @action("create_document")
    def create_document(self, data):
        """
        Создает документ в базе данных.
        """
        username = data.get('username')
        status = data.get('status')
        if not username or not status:
            raise ValueError("Некорректные данные: 'username' и 'status' обязательны.")
        self.db_client.create_activity(username, status)
        return {"message": f"Документ для пользователя '{username}' успешно создан."}

    @action("get_document")
    def get_document(self, request):
        """
        Получает документ из базы данных.
        """
        return self.db_client.get_activity(
            username=request.get("username"),
            status=request.get("status"),
            creation_date_start=request.get("creation_date_start"),
            creation_date_end=request.get("creation_date_end"),
        )
        
    @action("get_last_document")
    def get_last_document(self, request):
        """
        Получает последнюю запись для указанного username.
        """
        username = request.get("username")
        if not username:
            raise ValueError("Поле 'username' обязательно для get_last_document.")
        
        return self.db_client.get_last_activity(username)
    
    @action("upsert_daily_report")
    def upsert_daily_report(self, request):
        """
        Получает последнюю запись для указанного username.
        """
        username = request.get("username")
        if not username:
            raise ValueError("Поле 'username' обязательно для upsert_daily_report.")
        
        most_visited_hour = request.get("most_visited_hour")
        if not most_visited_hour:
            raise ValueError("Поле 'most_visited_hour' обязательно для upsert_daily_report.")
        
        total = request.get("total")
        if not total:
            raise ValueError("Поле 'total' обязательно для upsert_daily_report.")
        
        return self.db_client.upsert_daily_report(username, most_visited_hour, total)
    
    @action("upsert_daily_report")
    def upsert_daily_report(self, request):
        """
        Получает последнюю запись для указанного username.
        """
        username = request.get("username")
        if not username:
            raise ValueError("Поле 'username' обязательно для upsert_daily_report.")
        
        most_visited_hour = request.get("most_visited_hour")
        if not most_visited_hour:
            raise ValueError("Поле 'most_visited_hour' обязательно для upsert_daily_report.")
        
        total = request.get("total")
        if not total:
            raise ValueError("Поле 'total' обязательно для upsert_daily_report.")
        
        return self.db_client.upsert_daily_report(username, most_visited_hour, total)
    
    @action("get_daily_report")
    def get_daily_report(self, request):
        """
        Получает последнюю запись для указанного username.
        """
        
        return self.db_client.get_daily_report(
                                               username=request.get("username"), 
                                               most_visited_hour=request.get("most_visited_hour"), 
                                               total=request.get("total"), 
                                               creation_date_start=request.get("creation_date_start"),
                                               creation_date_end=request.get("creation_date_end")
                                               )