from prefect import flow, task
@task
def speak(message: str):
    print(message)
@flow(log_prints=True, name="prefect-example-hello-flow")
def hi():
    speak(message="Hi from Prefect! 🤗")
if __name__ == "__main__":
    hi()