core/
├── modules/
│   ├── assembly/
│   │   ├── api/
│   │   │   ├── routes.py
│   │   │   └── schemas.py
│   │   ├── domain/
│   │   │   ├── entities.py
│   │   │   ├── value_objects.py
│   │   │   └── rules.py
│   │   ├── application/
│   │   │   ├── services.py
│   │   │   ├── processors.py
│   │   │   └── commands.py
│   │   └── infrastructure/
│   │       ├── repository.py
│   │       ├── queries.py
│   │       ├── models.py
│   │       └── mappers.py
│   │
│   ├── consumption/
│   │   ├── api/
│   │   ├── domain/
│   │   ├── application/
│   │   └── infrastructure/
│   │
│   ├── forecast/
│   │   ├── api/
│   │   ├── domain/
│   │   ├── application/
│   │   └── infrastructure/
│   │
│   ├── requests_builder/
│   │   ├── api/
│   │   ├── domain/
│   │   ├── application/
│   │   └── infrastructure/
│   │
│   ├── requests_checker/
│   │   ├── api/
│   │   ├── domain/
│   │   ├── application/
│   │   └── infrastructure/
│   │
│   ├── requests_closure/
│   │   ├── api/
│   │   ├── domain/
│   │   ├── application/
│   │   └── infrastructure/
│   │
│   └── sap_manager/
│       ├── api/
│       ├── domain/
│       ├── application/
│       └── infrastructure/
│
├── common/
│   ├── utils/
│   │   ├── cleaner.py
│   │   ├── loader.py
│   │   └── converters.py
│   ├── exceptions/
│   │   └── http_exception.py
│   ├── logger/
│   │   └── logger.py
│   ├── middleware/
│   └── security/
│
├── config/
│   ├── settings.py
│   └── .env.example
│
├── database/
│   ├── engine.py
│   ├── session.py
│   └── migrations/
│       ├── env.py
│       ├── script.py.mako
│       └── versions/
│
├── integrations/
│   ├── sap/
│   │   ├── client.py
│   │   └── session_manager.py
│   └── external_api/
│
├── storage/
│   ├── raw/
│   ├── processed/
│   ├── logs/
│   │   ├── assembly/
│   │   ├── consumption/
│   │   ├── forecast/
│   │   ├── database/
│   │   ├── requests_builder/
│   │   ├── requests_checker/
│   │   ├── requests_closure/
│   │   └── sap_manager/
│   └── sap/
│       └── lt22.txt
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
├── main.py
├── requirements.txt
└── .gitignore