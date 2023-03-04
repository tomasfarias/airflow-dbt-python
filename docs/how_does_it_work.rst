.. _how_does_it_work:

How does it work?
=================

*airflow-dbt-python*'s main goal is to elevate *dbt* to **first-class citizen** status in Airflow. By this we mean that users of *dbt* can leverage as many Airflow features as possible, without breaking the assumptions that Airflow expects from any workflows it orchestrates. Perhaps more importantly, Airflow should **enhance** a *dbt* user's experience and not simply emulate the way they would run *dbt* in the command line. This is what separates *airflow-dbt-python* from other alternatives like *airflow-dbt* which simply wrap *dbt* cli commands in ``BashOperator``.

To achieve this goal *airflow-dbt-python* provides Airflow operators, hooks, and other utilities. Hooks in particular come in two flavors:

* A ``DbtHook`` that abstracts all interaction with *dbt* internals.
* Subclasses of ``DbtRemoteHook`` that expose an interface to interact with *dbt* remote storages where project files are located (like AWS S3 buckets or git repositories).

.. graphviz:: how_does_it_work.dot

*dbt* as a library
------------------

A lot of the code in *airflow-dbt-python* is required to provide a `wrapper <https://en.wikipedia.org/wiki/Adapter_pattern>`_ for *dbt*, as *dbt* only provides a CLI interface. There are `ongoing efforts <https://github.com/dbt-labs/dbt-core/issues/6356>`_ to provide a dbt library, which would significantly simplify our codebase. As of the time of development, these efforts are not in a state where they can be used by us, but we can keep an eye out for the future.

Most of the code used to adapting *dbt* can be found in the utilities module, as some of our features require that we break some assumptions *dbt* makes when initializing. For example, we need setup *dbt* to access project files stored remotely, or intiailize all profile settings from an Airflow Connection.

.. _dbt_operators:

*dbt* operators
---------------

*airflow-dbt-python* provides one operator per *dbt* task: for example, ``DbtRunOperator`` can be used to execute a *dbt* run command, as if running ``dbt run ...`` in the CLI.

.. _dbt_hooks:

*dbt* hooks
-----------

*airflow-dbt-python* provides a ``DbtHook`` to abstract all interactions with dbt. The main method of ``DbtHook`` operators should be calling is ``DbtHook.run_dbt_task`` which takes any *dbt* command as it's first argument, and any configuration parameters keyword arguments. This hook abstracts interactions with *dbt*, including:

* Setting up a temporary directory for *dbt* execution.
  * Potentially downloading files from a *dbt* remote into this directory.
* Using Airflow connections to configure *dbt* connections ("targets" as these connections are found in *profiles.yml*).
* Initializing a configuration for *dbt* with parameters provided.
  * Includes configuring *dbt* logging.

.. _independent-task-execution:

Temporary directories ensure task independence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow executes `tasks <https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html>`_ independent of one another: even though downstream and upstream dependencies between tasks exist, the execution of an individual task happens entirely independently of any other task execution (see: `Tasks Relationships <https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships>`_).

In order to respect this constraint, *airflow-dbt-python* hooks run each *dbt* command in a temporary and isolated directory:

1. Before execution, all the relevant *dbt* files are downloaded from supported remotes.
2. After execution, any resulting artifacts are uploaded back to supported remotes (if configured).

This ensures *dbt* can work with any Airflow deployment, including most production deployments running `Remote Executors <https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html#executor-types>`_ that do not guarantee any files will be shared between tasks, since each task may run in a completely different worker.

.. _dbt_remote_hooks:

*dbt* remote hooks
------------------

*dbt* remote hooks implement a simple interface to communicate with *dbt* remotes. A *dbt* remote can be any external storage that contains a *dbt* project and potentially also a *profiles.yml* file for example: an AWS S3 bucket or a GitHub repository. See the reference for a list of which remotes are currently supported.

Implementing the ``DbtRemoteHook`` interface
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Supporting a new remote to store *dbt* files requires implementing the ``DbtRemoteHook`` interface. There are only two methods in the interface: ``DbtRemoteHook.download`` and ``DbtRemoteHook.upload``.
