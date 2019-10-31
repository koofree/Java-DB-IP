from locust import HttpLocust, TaskSet, task


class WebsiteTasks(TaskSet):
    def on_start(self):
        pass

    @task
    def api_phone(self):
        with self.client.get("/1.214.48.230", catch_response=True) as res:
            if res.status_code == 400:
                res.success()


class WebsiteUser(HttpLocust):
    task_set = WebsiteTasks
    min_wait = 5000
    max_wait = 15000
