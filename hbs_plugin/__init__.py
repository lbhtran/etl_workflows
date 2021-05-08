from airflow.plugins_manager import AirflowPlugin
# from hbs_plugin.hooks import HbsHook
# from hbs_plugin.operators import CreateCustomerOperator, DeleteCustomerOperator, UpdateCustomerOperator


class AirflowHbsPlugin(AirflowPlugin):
    name = "hbs_plugin"  # does not need to match the package name
    operators = []
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []